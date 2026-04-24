/* global GalaxyBackground */
(function () {
  'use strict';

  (function checkLogin() {
    const token = localStorage.getItem('token');
    if (!token) window.location.replace('./login.html');
  })();

  const chat = document.getElementById('chat');
  const input = document.getElementById('input');
  const sendBtn = document.getElementById('sendBtn');
  const newSessionBtn = document.getElementById('newSessionBtn');
  const logoutBtn = document.getElementById('logoutBtn');
  const currentUserEl = document.getElementById('currentUser');
  const btnEvidence = document.getElementById('btnEvidence');
  const evidencePanel = document.getElementById('evidencePanel');
  const sourcePanel = document.getElementById('sourcePanel');
  const refreshSourcesBtn = document.getElementById('refreshSourcesBtn');
  const agentStatus = document.getElementById('agentStatus');
  const agentError = document.getElementById('agentError');
  const chartPanel = document.getElementById('chartPanel');
  const toggleChartBtn = document.getElementById('toggleChartBtn');
  const collapseChartBtn = document.getElementById('collapseChartBtn');
  const flowHint = document.getElementById('flowHint');
  const flowHintText = document.getElementById('flowHintText');
  const uploadBtn = document.getElementById('uploadBtn');
  const fileInput = document.getElementById('fileInput');
  const downloadPdfBtn = document.getElementById('downloadPdfBtn');
  const chatInputWrap = document.querySelector('.chat-input');

  let currentSessionId = '';
  const DEFAULT_TIME_RANGE = '近3年';
  let userCollapsedChartThisSession = false;
  let analysisStartTime = 0;
  /** 分析阶段最短停留（与坍缩 800ms 独立）：避免接口极快时用户看不到“分析中” */
  const MIN_ANALYZE_MS_BEFORE_COLLAPSE = 1000;
  let flowTimer = null;
  let flowStepIndex = 0;
  const FLOW_STEPS = [
    '总控Agent 正在拆解问题...',
    '分析师正在提取关键指标...',
    '证据分析师正在校验证据...',
    '决策分析师正在生成结论...',
  ];
  const TECH_THEME = {
    primary: '#4f8cff',
    secondary: '#34d399',
    accent: '#f59e0b',
    danger: '#ef4444',
    muted: '#94a3b8',
  };
  const CHART_THEME = TECH_THEME;
  let lastRenderedChartState = null;
  let currentEvidence = [];
  let isSending = false;
  let activeRequestId = 0;
  let fullscreenCard = null;

  const galaxy = (function () {
    try {
      if (!window.GalaxyBackground) return null;
      return window.GalaxyBackground.create();
    } catch (e) {
      append('sys', '背景渲染初始化失败：' + (e && e.message ? e.message : 'unknown'));
      return null;
    }
  })();

  // 若 three/galaxy 脚本未生效，给出明确提示，避免误以为“土星被隐藏”
  setTimeout(function () {
    try {
      if (!window.THREE) append('sys', '背景提示：THREE 未加载（请 Ctrl+F5 或检查网络/CDN）。');
      else if (!window.GalaxyBackground) append('sys', '背景提示：GalaxyBackground 未加载（galaxy.js 未执行）。');
      else if (!window.__GALAXY_READY__) append('sys', '背景提示：土星初始化未完成（可能被缓存旧脚本）。');
    } catch (_) {}
  }, 80);

  function safeText(v, fallback) {
    const f = fallback === undefined ? '-' : fallback;
    if (v === undefined || v === null || v === '') return f;
    return String(v);
  }

  function append(role, text) {
    const d = document.createElement('div');
    d.className = 'msg ' + role;
    d.textContent = text;
    chat.appendChild(d);
    chat.scrollTop = chat.scrollHeight;
  }

  function showToast(msg, type) {
    const toast = document.createElement('div');
    toast.textContent = msg;
    toast.style.position = 'fixed';
    toast.style.left = '50%';
    toast.style.top = '14px';
    toast.style.transform = 'translateX(-50%)';
    toast.style.zIndex = '99';
    toast.style.padding = '8px 14px';
    toast.style.borderRadius = '10px';
    toast.style.fontSize = '12px';
    toast.style.color = '#fff';
    toast.style.background = type === 'error' ? 'rgba(220,38,38,.9)' : 'rgba(30,64,175,.9)';
    document.body.appendChild(toast);
    setTimeout(function () {
      toast.remove();
    }, 2200);
  }

  function renderSources(sources) {
    if (!sources || !sources.length) {
      sourcePanel.textContent = '暂无证据';
      btnEvidence.textContent = '证据';
      return;
    }
    btnEvidence.textContent = '证据 (' + sources.length + ')';
    sourcePanel.textContent = sources
      .map(function (item, i) {
        return [
          i + 1 + '. [' + safeText(item.evidence_id) + '] ' + safeText(item.title),
          '来源: ' + safeText(item.source_type) + '/' + safeText(item.source),
          '置信度: ' + safeText(item.confidence),
          '链接: ' + safeText(item.url_or_path, 'N/A'),
          '摘要: ' + safeText(item.excerpt),
        ].join('\n');
      })
      .join('\n\n');
  }

  function renderAnswerMessage(answer) {
    const intent = answer.intent_type || 'analysis';
    const map = { low: '低', medium: '中', high: '高', unknown: '未知' };
    if (intent === 'chat') return '助手回复：' + safeText(answer.user_facing_reply, safeText(answer.summary));
    const reasons =
      (answer.key_findings || [])
        .slice(0, 4)
        .map(function (x, i) {
          return i + 1 + '. ' + safeText(x);
        })
        .join('\n') || '-';
    if (intent === 'decision') {
      return [
        '结论：' + safeText(answer.final_decision, '谨慎观望'),
        '解释：' + safeText(answer.user_facing_reply, safeText(answer.summary)),
        '置信度：' + safeText(answer.decision_confidence, 'medium'),
        '核心依据：\n' + reasons,
      ].join('\n\n');
    }
    return [
      '结论：' + safeText(answer.summary),
      '解释：' + safeText(answer.user_facing_reply, safeText(answer.summary)),
      answer.risk_level && answer.risk_level !== 'unknown' ? '风险等级：' + (map[answer.risk_level] || answer.risk_level) : '',
      '核心依据：\n' + reasons,
    ]
      .filter(Boolean)
      .join('\n\n');
  }

  function refreshSessionSources() {
    renderSources(currentEvidence);
    return Promise.resolve();
  }

  function isChartCollapsed() {
    return localStorage.getItem('chartPanelCollapsed') === '1';
  }
  function syncChartButtons() {
    const collapsed = isChartCollapsed();
    collapseChartBtn.textContent = collapsed ? '展开' : '收起';
    toggleChartBtn.textContent = collapsed ? '展开左侧' : '收起左侧';
  }
  function setChartCollapsed(collapsed, byUser) {
    if (!chartPanel) return;
    localStorage.setItem('chartPanelCollapsed', collapsed ? '1' : '0');
    if (byUser) userCollapsedChartThisSession = !!collapsed;
    if (collapsed) {
      chartPanel.classList.remove('show');
      if (galaxy) galaxy.playAppear();
    } else {
      if (byUser && galaxy) galaxy.hideForPanelOpen();
      chartPanel.classList.add('show');
    }
    syncChartButtons();
  }

  /**
   * 新一次分析前：若大屏已展开，必须先关闭，再让土星以 appear 出现，再进入分析中。
   * 在 POST 发出之前通过 callback 进入后续逻辑（含设置 analysisStartTime）。
   */
  function prepareForNewQuery(next) {
    if (typeof next !== 'function') return;
    if (chartPanel && chartPanel.classList.contains('show')) {
      chartPanel.classList.remove('show');
      localStorage.setItem('chartPanelCollapsed', '1');
      syncChartButtons();
      if (galaxy) {
        requestAnimationFrame(function () {
          galaxy.playAppear(function () {
            if (galaxy) galaxy.setAnalyzing();
            next();
          });
        });
      } else {
        next();
      }
      return;
    }
    if (galaxy) {
      if (galaxy.getAppState && galaxy.getAppState() === 'hidden') {
        galaxy.playAppear(function () {
          if (galaxy) galaxy.setAnalyzing();
          next();
        });
        return;
      }
      galaxy.setAnalyzing();
    }
    next();
  }

  function schedulePanelReveal(requestId) {
    var wait = Math.max(0, MIN_ANALYZE_MS_BEFORE_COLLAPSE - (Date.now() - analysisStartTime));
    setTimeout(function () {
      if (requestId !== activeRequestId) return;
      function doReveal() {
        if (requestId !== activeRequestId) return;
        if (!userCollapsedChartThisSession) {
          setChartCollapsed(false, false);
        }
        adjustChartLayout();
      }
      if (!galaxy) {
        doReveal();
        return;
      }
      galaxy.playCollapse(function () {
        doReveal();
      });
    }, wait);
  }

  function handleAgentQueryResult(result, requestId) {
    if (requestId !== activeRequestId) return;
    currentSessionId = result.session_id || currentSessionId;

    if (result.status === 'needs_clarification') {
      playFlowHint('问题解析完成，等待补充信息...');
      const qs = (result.clarification && result.clarification.questions) || [];
      append(
        'bot',
        '需要补充信息后才能继续：\n' +
          qs
            .map(function (q, i) {
              return i + 1 + '. ' + q.question;
            })
            .join('\n')
      );
      finishFlowProgress('需补充信息').catch(function () {});
      if (chartPanel) {
        chartPanel.classList.remove('show');
      }
      localStorage.setItem('chartPanelCollapsed', '1');
      syncChartButtons();
      if (galaxy) galaxy.setIdle();
      currentEvidence = result.evidence || [];
      renderSources(currentEvidence);
      agentStatus.textContent = '需补充信息';
      return;
    }

    if (result.status === 'completed') {
      if (downloadPdfBtn) {
        downloadPdfBtn.style.display = 'inline-block';
      } else {
        console.warn('downloadPdfBtn 不存在，跳过显示');
      }
      if (!result.report || Object.keys(result.report).length === 0) {
        if (agentError) agentError.textContent = '分析报告生成失败，请稍后重试或更换查询';
        append('sys', '分析报告生成失败：report 为空');
        if (galaxy) galaxy.setIdle();
        finishFlowProgress('分析完成').catch(function () {});
        currentEvidence = result.evidence || [];
        renderSources(currentEvidence);
        agentStatus.textContent = '响应完成';
        return;
      }
    }

    const answer = {};
    if ((result.evidence || []).length > 0) {
      playFlowHint('证据提取完成，正在生成结论...');
    } else {
      playFlowHint('正在整理分析结论...');
    }
    const sections = (result.report && result.report.sections) || {};
    const isFastPath = sections.mode === 'simple_metric_fast_path';
    if (isFastPath && result.charts) {
      const directMetricText = buildFastPathMetricText(result.charts);
      if (directMetricText) append('bot', directMetricText);
      renderCharts(result.charts, answer);
    } else {
      renderReportMessage(result);
    }
    if (!isFastPath) {
      finishLoading(answer, result.charts);
    }
    finishFlowProgress('分析完成').catch(function () {});
    schedulePanelReveal(requestId);
    currentEvidence = result.evidence || [];
    renderSources(currentEvidence);
    agentStatus.textContent = '响应完成';
  }

  function playFlowHint(text) {
    if (!flowHint || !flowHintText) return;
    flowHint.classList.remove('fade-out');
    flowHint.classList.add('show');
    flowHintText.textContent = text;
    flowHintText.classList.remove('flash');
    void flowHintText.offsetWidth;
    flowHintText.classList.add('flash');
  }

  function startFlowProgress() {
    if (!flowHint || !flowHintText) return;
    stopFlowProgressInstant();
    flowStepIndex = 0;
    playFlowHint(FLOW_STEPS[flowStepIndex]);
    // 按真实请求生命周期单向前进，不循环，避免重复文字显得不专业。
    flowTimer = setTimeout(function () {
      flowStepIndex = 1;
      playFlowHint(FLOW_STEPS[flowStepIndex]);
      flowTimer = setTimeout(function () {
        flowStepIndex = 2;
        playFlowHint(FLOW_STEPS[flowStepIndex]);
        flowTimer = setTimeout(function () {
          flowStepIndex = 3;
          playFlowHint(FLOW_STEPS[flowStepIndex]);
        }, 1500);
      }, 1200);
    }, 800);
  }

  function stopFlowProgressInstant() {
    if (flowTimer) {
      clearInterval(flowTimer);
      clearTimeout(flowTimer);
      flowTimer = null;
    }
    if (!flowHint) return;
    flowHint.classList.remove('show');
    flowHint.classList.remove('fade-out');
  }

  function finishFlowProgress(finalText) {
    if (!flowHint || !flowHintText) return Promise.resolve();
    if (flowTimer) {
      clearInterval(flowTimer);
      clearTimeout(flowTimer);
      flowTimer = null;
    }
    playFlowHint(finalText || '分析完成');
    return new Promise(function (resolve) {
      setTimeout(function () {
        flowHint.classList.add('fade-out');
        setTimeout(function () {
          flowHint.classList.remove('show');
          flowHint.classList.remove('fade-out');
          resolve();
        }, 520);
      }, 420);
    });
  }

  function escapeHtml(s) {
    return String(s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function normalizeCharts(charts) {
    const safe = charts && typeof charts === 'object' ? charts : {};
    const radar = safe.radar && typeof safe.radar === 'object' ? safe.radar : {};
    const bar = safe.bar && typeof safe.bar === 'object' ? safe.bar : {};
    const line = safe.line && typeof safe.line === 'object' ? safe.line : {};
    const gauge = safe.gauge && typeof safe.gauge === 'object' ? safe.gauge : {};
    return {
      radar: {
        indicators: Array.isArray(radar.indicators) ? radar.indicators : [],
        series: Array.isArray(radar.series) ? radar.series : [],
      },
      bar: {
        categories: Array.isArray(bar.categories) ? bar.categories : [],
        series: Array.isArray(bar.series) ? bar.series : [],
      },
      line: {
        categories: Array.isArray(line.categories) ? line.categories : [],
        series: Array.isArray(line.series) ? line.series : [],
      },
      gauge: {
        value: Number.isFinite(Number(gauge.value)) ? Number(gauge.value) : 0,
      },
    };
  }

  function renderNoDataPlaceholder(title, desc) {
    return (
      '<div class="chart-box">' +
      '<h3 style="margin:0 0 8px 0;color:' +
      CHART_THEME.muted +
      ';">' +
      escapeHtml(title) +
      '</h3>' +
      '<p style="margin:0;color:#cbd5e1;">' +
      escapeHtml(desc || '暂无可展示数据') +
      '</p>' +
      '</div>'
    );
  }

  function renderSimpleBars(categories, values, color) {
    if (!categories.length || !values.length) return renderNoDataPlaceholder('柱状图', '当前查询未返回可比较维度');
    const maxVal = Math.max.apply(
      null,
      values.map(function (v) {
        return Number(v) || 0;
      })
    );
    return (
      '<div class="chart-box"><h3 style="margin:0 0 8px 0;">证据分布</h3>' +
      categories
        .map(function (c, idx) {
          const v = Number(values[idx]) || 0;
          const width = maxVal > 0 ? Math.max(5, Math.round((v / maxVal) * 100)) : 5;
          return (
            '<div style="margin:8px 0;">' +
            '<div style="display:flex;justify-content:space-between;font-size:12px;color:#d1d5db;"><span>' +
            escapeHtml(c) +
            '</span><span>' +
            v +
            '</span></div>' +
            '<div style="height:8px;background:rgba(255,255,255,.08);border-radius:99px;overflow:hidden;">' +
            '<div style="height:100%;width:' +
            width +
            '%;background:' +
            color +
            ';"></div>' +
            '</div>' +
            '</div>'
          );
        })
        .join('') +
      '</div>'
    );
  }

  function renderLineSummary(categories, values) {
    if (!categories.length || !values.length) return renderNoDataPlaceholder('趋势图', '暂无时间序列数据');
    const points = categories
      .map(function (c, i) {
        return escapeHtml(c) + ': ' + (Number(values[i]) || 0);
      })
      .join(' | ');
    return '<div class="chart-box"><h3 style="margin:0 0 8px 0;">时间趋势</h3><p style="margin:0;color:#d1d5db;">' + points + '</p></div>';
  }

  function renderRadarSummary(indicators, values) {
    if (!indicators.length || !values.length) return renderNoDataPlaceholder('雷达图', '暂无能力维度评分');
    const lines = indicators
      .map(function (it, i) {
        const n = typeof it === 'object' ? it.name : String(it);
        return '<div style="display:flex;justify-content:space-between;margin:6px 0;"><span>' + escapeHtml(n) + '</span><span style="color:' + CHART_THEME.secondary + ';">' + (Number(values[i]) || 0) + '</span></div>';
      })
      .join('');
    return '<div class="chart-box"><h3 style="margin:0 0 8px 0;">综合能力雷达</h3>' + lines + '</div>';
  }

  function renderGauge(value) {
    const score = Math.max(0, Math.min(100, Number(value) || 0));
    const color = score >= 75 ? CHART_THEME.secondary : score >= 50 ? CHART_THEME.accent : CHART_THEME.danger;
    return (
      '<div class="chart-box"><h3 style="margin:0 0 8px 0;">综合评分</h3>' +
      '<div style="font-size:30px;font-weight:700;color:' +
      color +
      ';">' +
      score.toFixed(1) +
      '</div>' +
      '<div style="height:8px;margin-top:8px;background:rgba(255,255,255,.08);border-radius:99px;overflow:hidden;">' +
      '<div style="height:100%;width:' +
      score +
      '%;background:' +
      color +
      ';"></div>' +
      '</div>' +
      '</div>'
    );
  }

  function renderEChartsGauge(el, gaugeInput) {
    if (!el || !window.echarts) return false;
    var payload =
      typeof gaugeInput === 'object' && gaugeInput !== null && !Array.isArray(gaugeInput)
        ? gaugeInput
        : { value: gaugeInput, rating: '', title: '综合评分' };
    var score = Math.max(0, Math.min(100, Number(payload.value) || 0));
    var titleText = payload.title ? String(payload.title) : '综合评分';
    var ratingStr = payload.rating != null && payload.rating !== '' ? String(payload.rating) : '';
    var chart = window.echarts.init(el, null, { renderer: 'canvas' });
    chart.setOption({
      backgroundColor: 'transparent',
      title: { text: titleText, left: 12, top: 10, textStyle: { color: '#e5e7eb', fontSize: 14 } },
      series: [
        {
          type: 'gauge',
          startAngle: 210,
          endAngle: -30,
          min: 0,
          max: 100,
          splitNumber: 5,
          progress: { show: true, width: 14 },
          axisLine: { lineStyle: { width: 14, color: [[0.5, CHART_THEME.danger], [0.75, CHART_THEME.accent], [1, CHART_THEME.secondary]] } },
          axisTick: { distance: -16, length: 6, lineStyle: { color: 'rgba(255,255,255,.4)' } },
          splitLine: { distance: -16, length: 12, lineStyle: { color: 'rgba(255,255,255,.5)' } },
          axisLabel: { color: 'rgba(255,255,255,.65)' },
          pointer: { show: true, length: '62%', width: 6 },
          anchor: { show: true, showAbove: true, size: 10, itemStyle: { color: CHART_THEME.primary } },
          detail: {
            valueAnimation: true,
            formatter: function (val) {
              var s = Number(val).toFixed(1);
              return ratingStr ? s + ' · ' + ratingStr : s;
            },
            color: '#fff',
            fontSize: 22,
            offsetCenter: [0, '65%'],
          },
          data: [{ value: Number(score.toFixed(1)) }],
        },
      ],
    });
    el.__chart__ = chart;
    return true;
  }

  function renderEChartsScatter(el, scatter) {
    if (!el || !window.echarts) return false;
    const series = (scatter && scatter.series) || [];
    const chart = window.echarts.init(el, null, { renderer: 'canvas' });
    chart.setOption({
      backgroundColor: 'transparent',
      title: { text: '风险-收益散点', left: 12, top: 10, textStyle: { color: '#e5e7eb', fontSize: 14 } },
      grid: { left: 46, right: 22, top: 56, bottom: 40 },
      xAxis: { type: 'value', name: '风险', nameTextStyle: { color: 'rgba(255,255,255,.7)' }, axisLabel: { color: 'rgba(255,255,255,.65)' }, splitLine: { lineStyle: { color: 'rgba(255,255,255,.08)' } } },
      yAxis: { type: 'value', name: '收益', nameTextStyle: { color: 'rgba(255,255,255,.7)' }, axisLabel: { color: 'rgba(255,255,255,.65)' }, splitLine: { lineStyle: { color: 'rgba(255,255,255,.08)' } } },
      tooltip: { trigger: 'item' },
      series: series.length
        ? series.map(function (s, idx) {
            return {
              type: 'scatter',
              name: s.name || ('企业' + (idx + 1)),
              data: Array.isArray(s.data) ? s.data : [],
              symbolSize: 14,
            };
          })
        : [
            {
              type: 'scatter',
              data: [],
            },
          ],
      legend: { top: 30, textStyle: { color: 'rgba(255,255,255,.75)' } },
    });
    el.__chart__ = chart;
    return true;
  }

  function ensureChartDom() {
    const content = document.getElementById('chartContent');
    if (!content) return null;
    let gaugeEl = document.getElementById('gaugeChart');
    let scatterEl = document.getElementById('scatterChart');
    let textEl = document.getElementById('chartTextBlocks');
    const radarEl = document.getElementById('radarChart');
    const barEl = document.getElementById('barChart');
    const lineEl = document.getElementById('lineChart');
    const stackedEl = document.getElementById('stackedBarChart');
    const heatmapEl = document.getElementById('heatmapChart');
    const wordcloudEl = document.getElementById('wordcloudChart');
    return { content: content, gaugeEl: gaugeEl, scatterEl: scatterEl, radarEl: radarEl, barEl: barEl, lineEl: lineEl, stackedEl: stackedEl, heatmapEl: heatmapEl, wordcloudEl: wordcloudEl, textEl: textEl };
  }

  function getChartGrid() {
    const content = document.getElementById('chartContent');
    if (!content) return null;
    let grid = document.getElementById('chartGrid');
    if (!grid) {
      grid = document.createElement('div');
      grid.id = 'chartGrid';
      content.appendChild(grid);
    }
    return grid;
  }

  function adjustChartLayout() {
    const chartGrid = document.getElementById('chartGrid');
    if (!chartGrid) return;
    const cards = Array.from(chartGrid.querySelectorAll('.chart-card'));
    if (cards.length === 1) {
      cards[0].classList.add('fullscreen-card');
      cards[0].style.height = '80vh';
      cards[0].style.width = '100%';
      resizeCardChart(cards[0]);
      return;
    }
    cards.forEach(function (card) {
      card.classList.remove('fullscreen-card');
      card.style.width = '';
      card.style.height = '350px';
      resizeCardChart(card);
    });
  }

  function resizeCardChart(card) {
    if (!card) return;
    const body = card.querySelector('.chart-card-body');
    if (!body) return;
    if (body.__chart__ && typeof body.__chart__.resize === 'function') {
      body.__chart__.resize();
    }
  }

  function toggleCardFullscreen(card) {
    if (!card) return;
    if (fullscreenCard && fullscreenCard !== card) {
      fullscreenCard.classList.remove('chart-card-fullscreen');
      resizeCardChart(fullscreenCard);
      fullscreenCard = null;
    }
    const entering = !card.classList.contains('chart-card-fullscreen');
    if (entering) {
      card.classList.add('chart-card-fullscreen');
      fullscreenCard = card;
    } else {
      card.classList.remove('chart-card-fullscreen');
      if (fullscreenCard === card) fullscreenCard = null;
    }
    resizeCardChart(card);
  }

  // v2.2 enhancement compatibility: dynamic chart card creation.
  function createChartCard(title, badge) {
    const wrap = document.createElement('div');
    wrap.className = 'chart-card chart-box';
    wrap.style.height = '350px';
    const header = document.createElement('div');
    header.className = 'chart-card-header';
    header.style.display = 'flex';
    header.style.justifyContent = 'space-between';
    header.style.alignItems = 'center';
    header.style.marginBottom = '8px';
    header.innerHTML =
      '<strong>' + escapeHtml(title || '图表') + '</strong>' +
      (badge ? '<span style="font-size:12px;color:#94a3b8;">' + escapeHtml(badge) + '</span>' : '');
    const body = document.createElement('div');
    body.className = 'chart-card-body';
    body.style.height = '290px';
    const resizer = document.createElement('div');
    resizer.className = 'chart-resizer';
    let resizing = false;
    let sx = 0;
    let sy = 0;
    let sw = 0;
    let sh = 0;
    resizer.addEventListener('mousedown', function (evt) {
      evt.preventDefault();
      evt.stopPropagation();
      resizing = true;
      sx = evt.clientX;
      sy = evt.clientY;
      sw = wrap.offsetWidth;
      sh = wrap.offsetHeight;
      document.body.style.userSelect = 'none';
    });
    window.addEventListener('mousemove', function (evt) {
      if (!resizing) return;
      const nextW = Math.max(400, sw + (evt.clientX - sx));
      const nextH = Math.max(280, sh + (evt.clientY - sy));
      wrap.style.width = nextW + 'px';
      wrap.style.height = nextH + 'px';
      body.style.height = Math.max(200, nextH - 60) + 'px';
      resizeCardChart(wrap);
    });
    window.addEventListener('mouseup', function () {
      if (!resizing) return;
      resizing = false;
      document.body.style.userSelect = '';
    });
    wrap.addEventListener('dblclick', function () {
      toggleCardFullscreen(wrap);
    });
    wrap.appendChild(header);
    wrap.appendChild(body);
    wrap.appendChild(resizer);
    return { card: wrap, box: body };
  }

  function renderRadarChart(el, radar) {
    if (!el || !window.echarts) return false;
    const inds = (radar && radar.indicators) || [];
    const series = (radar && radar.series) || [];
    const palette = [CHART_THEME.primary, CHART_THEME.secondary, CHART_THEME.accent, '#d6b97a', '#a78bfa'];
    const radarData = series
      .map(function (s, idx) {
        const values = Array.isArray(s.value) ? s.value : [];
        return { value: values, name: s.name || '企业' + (idx + 1), color: palette[idx % palette.length] };
      })
      .filter(function (d) {
        return d.value.length > 0;
      });
    if (!inds.length || !radarData.length) {
      el.innerHTML = renderNoDataPlaceholder('雷达图', '暂无能力维度数据');
      return true;
    }
    const chart = window.echarts.init(el, null, { renderer: 'canvas' });
    chart.setOption({
      backgroundColor: 'transparent',
      title: { text: '综合能力雷达', left: 12, top: 10, textStyle: { color: '#e5e7eb', fontSize: 14 } },
      legend:
        radarData.length > 1
          ? { top: 32, textStyle: { color: 'rgba(255,255,255,.75)' }, data: radarData.map(function (d) { return d.name; }) }
          : undefined,
      radar: {
        indicator: inds.map(function (it) {
          if (typeof it === 'object') return { name: it.name || '-', max: it.max || 100 };
          return { name: String(it), max: 100 };
        }),
        splitLine: { lineStyle: { color: 'rgba(255,255,255,.08)' } },
        splitArea: { areaStyle: { color: ['rgba(255,255,255,.02)'] } },
        axisName: { color: 'rgba(255,255,255,.7)' },
      },
      series: [
        {
          type: 'radar',
          data: radarData.map(function (d) {
            return {
              value: d.value,
              name: d.name,
              areaStyle: { opacity: 0.12 },
              lineStyle: { color: d.color },
              itemStyle: { color: d.color },
            };
          }),
        },
      ],
    });
    el.__chart__ = chart;
    return true;
  }

  function renderBarChart(el, bar) {
    if (!el || !window.echarts) return false;
    const cats = (bar && bar.categories) || [];
    const series = (bar && bar.series) || [];
    const first = series[0] || {};
    const data = Array.isArray(first.data) ? first.data : [];
    if (!cats.length || !data.length) {
      el.innerHTML = renderNoDataPlaceholder('柱状图', '暂无对比数据');
      return true;
    }
    const chart = window.echarts.init(el, null, { renderer: 'canvas' });
    chart.setOption({
      backgroundColor: 'transparent',
      title: { text: '对比柱状图', left: 12, top: 10, textStyle: { color: '#e5e7eb', fontSize: 14 } },
      grid: { left: 56, right: 18, top: 56, bottom: 42 },
      xAxis: { type: 'category', data: cats, axisLabel: { color: 'rgba(255,255,255,.65)' }, axisLine: { lineStyle: { color: 'rgba(255,255,255,.15)' } } },
      yAxis: { type: 'value', axisLabel: { color: 'rgba(255,255,255,.65)' }, splitLine: { lineStyle: { color: 'rgba(255,255,255,.08)' } } },
      series: [{ type: 'bar', data: data, itemStyle: { color: CHART_THEME.primary }, barMaxWidth: 24 }],
      tooltip: { trigger: 'axis' },
    });
    el.__chart__ = chart;
    return true;
  }

  function renderRankingBarChart(rankingBar, chartGrid, cardTitle) {
    if (!chartGrid || !window.echarts) return false;
    const categories = (rankingBar && rankingBar.categories) || [];
    const series = (rankingBar && rankingBar.series) || [];
    if (!categories.length || !series.length) return false;
    const ratings = (rankingBar && rankingBar.ratings) || [];
    const c = createChartCard(cardTitle || '🏆 综合排名', 'comparison_ranking');
    chartGrid.appendChild(c.card);
    const inst = window.echarts.init(c.box, null, { renderer: 'canvas' });
    const values = Array.isArray(series[0].data) ? series[0].data : [];
    const barData = values.map(function (v, idx) {
      var num = typeof v === 'object' && v !== null && 'value' in v ? Number(v.value) : Number(v);
      var r =
        (typeof v === 'object' && v !== null && v.rating != null && v.rating !== ''
          ? String(v.rating)
          : ratings[idx] != null
            ? String(ratings[idx])
            : '') || '';
      return {
        value: num,
        rating: r,
        itemStyle: {
          color: idx === 0 ? '#d6b97a' : CHART_THEME.primary,
        },
      };
    });
    inst.setOption({
      backgroundColor: 'transparent',
      grid: { left: 70, right: 56, top: 16, bottom: 30, containLabel: true },
      xAxis: { type: 'value', axisLabel: { color: 'rgba(255,255,255,.65)' }, splitLine: { lineStyle: { color: 'rgba(255,255,255,.08)' } } },
      yAxis: { type: 'category', data: categories, axisLabel: { color: 'rgba(255,255,255,.72)' }, axisLine: { lineStyle: { color: 'rgba(255,255,255,.15)' } } },
      series: [
        {
          type: 'bar',
          data: barData,
          label: {
            show: true,
            position: 'right',
            color: 'rgba(255,245,225,.92)',
            formatter: function (params) {
              var d = params.data;
              var r = d && d.rating ? d.rating : '';
              var val = d && typeof d.value !== 'undefined' ? d.value : params.value;
              var num = Number(val);
              if ((!num && num !== 0) || (num === 0 && (!r || r === '-'))) {
                return '暂无本地评分';
              }
              return r ? num + '  ' + r : String(num);
            },
          },
        },
      ],
      tooltip: { trigger: 'axis' },
    });
    return true;
  }

  function renderMetricSeriesCard(metricSeries) {
    if (!window.echarts) return false;
    const chartGrid = getChartGrid();
    if (!chartGrid) return false;
    const metricName = metricDisplayName(metricSeries.metric);
    const c = createChartCard('📈 ' + metricName + '趋势', 'simple_metric');
    chartGrid.appendChild(c.card);
    const categories = Array.isArray(metricSeries.categories) ? metricSeries.categories : [];
    const s0 = (metricSeries.series && metricSeries.series[0]) || { name: '指标', data: [] };
    const preferredType = metricSeries.type === 'line' || metricSeries.type === 'bar' ? metricSeries.type : null;
    const inst = window.echarts.init(c.box, null, { renderer: 'canvas' });
    inst.setOption({
      backgroundColor: 'transparent',
      grid: { left: 56, right: 18, top: 30, bottom: 42 },
      xAxis: { type: 'category', data: categories, axisLabel: { color: 'rgba(255,255,255,.65)' }, axisLine: { lineStyle: { color: 'rgba(255,255,255,.15)' } } },
      yAxis: { type: 'value', axisLabel: { color: 'rgba(255,255,255,.65)' }, splitLine: { lineStyle: { color: 'rgba(255,255,255,.08)' } } },
      series: [{ type: preferredType || (categories.length > 1 ? 'line' : 'bar'), data: Array.isArray(s0.data) ? s0.data : [], smooth: true, itemStyle: { color: CHART_THEME.primary }, lineStyle: { color: CHART_THEME.primary } }],
      tooltip: { trigger: 'axis' },
    });
    return true;
  }

  function renderLineChart(el, line) {
    if (!el || !window.echarts) return false;
    const cats = (line && line.categories) || [];
    const series = (line && line.series) || [];
    const first = series[0] || {};
    const data = Array.isArray(first.data) ? first.data : [];
    if (!cats.length || !data.length) {
      el.innerHTML = renderNoDataPlaceholder('折线图', '暂无趋势数据');
      return true;
    }
    const chart = window.echarts.init(el, null, { renderer: 'canvas' });
    chart.setOption({
      backgroundColor: 'transparent',
      title: { text: '趋势折线', left: 12, top: 10, textStyle: { color: '#e5e7eb', fontSize: 14 } },
      grid: { left: 56, right: 18, top: 56, bottom: 42 },
      xAxis: { type: 'category', data: cats, axisLabel: { color: 'rgba(255,255,255,.65)' }, axisLine: { lineStyle: { color: 'rgba(255,255,255,.15)' } } },
      yAxis: { type: 'value', axisLabel: { color: 'rgba(255,255,255,.65)' }, splitLine: { lineStyle: { color: 'rgba(255,255,255,.08)' } } },
      series: [{ type: 'line', data: data, smooth: true, symbolSize: 6, lineStyle: { color: CHART_THEME.secondary }, itemStyle: { color: CHART_THEME.secondary } }],
      tooltip: { trigger: 'axis' },
    });
    el.__chart__ = chart;
    return true;
  }

  function renderHeatmap(el, heatmap) {
    if (!el || !window.echarts) return false;
    const cats = (heatmap && heatmap.categories) || [];
    const vals = (heatmap && heatmap.values) || [];
    if (!cats.length || !vals.length) {
      el.innerHTML = renderNoDataPlaceholder('热力图', '暂无热力分布');
      return true;
    }
    // render as category/value bars-like heat
    const data = cats.map(function (c, i) { return [c, Number(vals[i]) || 0]; });
    const chart = window.echarts.init(el, null, { renderer: 'canvas' });
    chart.setOption({
      backgroundColor: 'transparent',
      title: { text: '热力分布', left: 12, top: 10, textStyle: { color: '#e5e7eb', fontSize: 14 } },
      grid: { left: 96, right: 18, top: 56, bottom: 18 },
      xAxis: { type: 'value', axisLabel: { color: 'rgba(255,255,255,.65)' }, splitLine: { lineStyle: { color: 'rgba(255,255,255,.08)' } } },
      yAxis: { type: 'category', data: cats, axisLabel: { color: 'rgba(255,255,255,.65)' }, axisLine: { lineStyle: { color: 'rgba(255,255,255,.15)' } } },
      series: [{ type: 'bar', data: data.map(function (x) { return x[1]; }), itemStyle: { color: CHART_THEME.danger }, barMaxWidth: 10 }],
      tooltip: { trigger: 'axis' },
    });
    el.__chart__ = chart;
    return true;
  }

  function renderStackedBar(el, stacked) {
    if (!el || !window.echarts) return false;
    const cats = (stacked && stacked.categories) || [];
    const series = (stacked && stacked.series) || [];
    if (!cats.length || !series.length) {
      el.innerHTML = renderNoDataPlaceholder('堆积柱状图', '暂无司法/结构数据');
      return true;
    }
    const colors = [CHART_THEME.primary, CHART_THEME.danger, CHART_THEME.accent, CHART_THEME.secondary];
    const s = series.map(function (x, idx) {
      return { type: 'bar', name: x.name || ('类型' + (idx + 1)), stack: 'total', data: Array.isArray(x.data) ? x.data : [], itemStyle: { color: colors[idx % colors.length] } };
    });
    const chart = window.echarts.init(el, null, { renderer: 'canvas' });
    chart.setOption({
      backgroundColor: 'transparent',
      title: { text: '结构堆积', left: 12, top: 10, textStyle: { color: '#e5e7eb', fontSize: 14 } },
      grid: { left: 56, right: 18, top: 56, bottom: 42 },
      xAxis: { type: 'category', data: cats, axisLabel: { color: 'rgba(255,255,255,.65)' }, axisLine: { lineStyle: { color: 'rgba(255,255,255,.15)' } } },
      yAxis: { type: 'value', axisLabel: { color: 'rgba(255,255,255,.65)' }, splitLine: { lineStyle: { color: 'rgba(255,255,255,.08)' } } },
      legend: { top: 30, textStyle: { color: 'rgba(255,255,255,.75)' } },
      series: s,
      tooltip: { trigger: 'axis' },
    });
    el.__chart__ = chart;
    return true;
  }

  function renderWordcloudChart(el, items) {
    if (!el) return false;
    // keep simple: tag cloud, avoid extra deps
    el.innerHTML = renderWordcloud(items);
    return true;
  }

  function renderCharts(charts, answer) {
    try {
      window.__LAST_CHARTS__ = charts;
      console.log('renderCharts called', charts);
    } catch (_) {}
    const content = document.getElementById('chartContent');
    const chartGrid = getChartGrid();
    if (!content || !chartGrid) {
      console.warn('chartContent 容器不存在，跳过渲染');
      return;
    }

    // Render immediately; avoid timer-driven re-render loops.
    const normalized = normalizeCharts(charts);
    // Keep raw charts payload for resize re-render.
    // Using normalized payload here can drop metric_series and clear simple_metric charts.
    lastRenderedChartState = { charts: charts || {}, answer: answer || {} };
    const chartType = (charts && charts.chart_type) || 'analysis';
    chartGrid.innerHTML = '';

    if (chartType === 'simple_metric' && charts && charts.metric_series) {
      renderMetricSeriesCard(charts.metric_series);
      adjustChartLayout();
      return;
    }
    if (chartType === 'analysis') {
      const radarCard = createChartCard('📊 综合能力雷达', 'analysis_radar');
      chartGrid.appendChild(radarCard.card);
      renderRadarChart(radarCard.box, (charts && charts.radar) || {});
      const scatterCard = createChartCard('🎯 风险-收益散点', 'analysis_scatter');
      chartGrid.appendChild(scatterCard.card);
      renderEChartsScatter(scatterCard.box, (charts && charts.scatter) || {});
      var g = charts && charts.gauge;
      if (g && typeof g === 'object' && Number.isFinite(Number(g.value))) {
        const gaugeCard = createChartCard(String(g.title || '综合评分'), 'analysis_gauge');
        chartGrid.appendChild(gaugeCard.card);
        renderEChartsGauge(gaugeCard.box, g);
      }
      adjustChartLayout();
      return;
    }
    if (chartType === 'ranking') {
      const barPayload = charts && charts.bar;
      if (barPayload && Array.isArray(barPayload.categories) && barPayload.categories.length) {
        const rb = {
          categories: barPayload.categories,
          series: barPayload.series && barPayload.series.length ? barPayload.series : [{ name: '指标', data: [] }],
          ratings: barPayload.ratings || [],
        };
        const ct = barPayload.metric_title ? '📊 ' + String(barPayload.metric_title) : '📊 排行榜';
        renderRankingBarChart(rb, chartGrid, ct);
      } else {
        const rc = createChartCard('📊 排行榜', 'ranking');
        chartGrid.appendChild(rc.card);
        rc.box.innerHTML = renderNoDataPlaceholder('排行榜', '暂无可展示的排行数据，请补充企业范围或稍后重试');
      }
      adjustChartLayout();
      return;
    }
    if (chartType === 'comparison_ranking') {
      if (charts && charts.ranking_bar) renderRankingBarChart(charts.ranking_bar, chartGrid);
      if (charts && charts.radar) {
        const rc = createChartCard('🕸️ 综合能力雷达', 'comparison_radar');
        chartGrid.appendChild(rc.card);
        renderRadarChart(rc.box, charts.radar);
      }
      if (charts && charts.scatter) {
        const sc = createChartCard('🎯 风险-收益散点', 'scatter');
        chartGrid.appendChild(sc.card);
        renderEChartsScatter(sc.box, charts.scatter);
      }
      adjustChartLayout();
      return;
    }
    if (chartType === 'legal_risk') {
      const legalCard = createChartCard('⚖️ 司法风险分析', 'legal_risk');
      chartGrid.appendChild(legalCard.card);
      legalCard.box.style.display = 'grid';
      legalCard.box.style.gridTemplateColumns = '1fr 1fr';
      legalCard.box.style.gap = '10px';
      const stackedEl = document.createElement('div');
      stackedEl.style.height = '100%';
      const heatmapEl = document.createElement('div');
      heatmapEl.style.height = '100%';
      legalCard.box.innerHTML = '';
      legalCard.box.appendChild(stackedEl);
      legalCard.box.appendChild(heatmapEl);
      renderStackedBar(stackedEl, (charts && charts.stacked_bar) || {});
      renderHeatmap(heatmapEl, (charts && charts.heatmap) || {});
      adjustChartLayout();
      return;
    }
    if (chartType === 'sentiment') {
      const sentimentCard = createChartCard('📰 舆情分析', 'sentiment');
      chartGrid.appendChild(sentimentCard.card);
      if (charts && charts.line && Array.isArray(charts.line.categories) && charts.line.categories.length) {
        renderLineChart(sentimentCard.box, charts.line);
      } else {
        var sHint = (charts && charts.sentiment_hint) || '暂无舆情数据可视化';
        sentimentCard.box.innerHTML = renderNoDataPlaceholder('舆情分析', String(sHint));
      }
      adjustChartLayout();
      return;
    }
    if (chartType === 'general') {
      const generalCard = createChartCard('通用分析摘要', 'general');
      chartGrid.appendChild(generalCard.card);
      const summary = answer && (answer.user_facing_reply || answer.summary);
      generalCard.box.innerHTML = summary
        ? '<div class=\"chart-box\"><p style=\"margin:0;color:#d1d5db;\">' + escapeHtml(summary) + '</p></div>'
        : renderNoDataPlaceholder('通用分析', '当前查询未返回可视化图表');
      adjustChartLayout();
      return;
    }

    // Generic analysis rendering: only create cards with real chart data.
    if (charts && charts.scatter && Array.isArray(charts.scatter.series) && charts.scatter.series.length) {
      const scatterCard = createChartCard('风险-收益散点', 'scatter');
      chartGrid.appendChild(scatterCard.card);
      renderEChartsScatter(scatterCard.box, charts.scatter);
    }
    if (charts && charts.radar && Array.isArray(charts.radar.series) && charts.radar.series.length) {
      const radarCard = createChartCard('综合能力雷达', 'radar');
      chartGrid.appendChild(radarCard.card);
      renderRadarChart(radarCard.box, charts.radar);
    }
    if (charts && charts.ranking_bar && Array.isArray(charts.ranking_bar.categories) && charts.ranking_bar.categories.length) {
      renderRankingBarChart(charts.ranking_bar, chartGrid);
    }
    if (charts && charts.bar && Array.isArray(charts.bar.categories) && charts.bar.categories.length) {
      const barCard = createChartCard('📊 对比柱状图', 'bar');
      chartGrid.appendChild(barCard.card);
      renderBarChart(barCard.box, charts.bar);
    }
    if (charts && charts.line && Array.isArray(charts.line.categories) && charts.line.categories.length) {
      const lineCard = createChartCard('📈 趋势折线', 'line');
      chartGrid.appendChild(lineCard.card);
      renderLineChart(lineCard.box, charts.line);
    }
    if (charts && charts.stacked_bar && Array.isArray(charts.stacked_bar.categories) && charts.stacked_bar.categories.length) {
      const stackedCard = createChartCard('结构堆积', 'stacked_bar');
      chartGrid.appendChild(stackedCard.card);
      renderStackedBar(stackedCard.box, charts.stacked_bar);
    }
    if (charts && charts.heatmap && Array.isArray(charts.heatmap.categories) && charts.heatmap.categories.length) {
      const heatmapCard = createChartCard('热力分布', 'heatmap');
      chartGrid.appendChild(heatmapCard.card);
      renderHeatmap(heatmapCard.box, charts.heatmap);
    }
    if (charts && charts.wordcloud && Array.isArray(charts.wordcloud) && charts.wordcloud.length) {
      const wordcloudCard = createChartCard('关键词词云', 'wordcloud');
      chartGrid.appendChild(wordcloudCard.card);
      renderWordcloudChart(wordcloudCard.box, charts.wordcloud);
    }
    adjustChartLayout();
  }

  function renderScatterChart(scatter) {
    const series = (scatter && scatter.series) || [];
    if (!series.length) return renderNoDataPlaceholder('散点图', '暂无风险-收益数据');
    const rows = series
      .map(function (s) {
        const p = (s.data && s.data[0]) || [0, 0];
        return '<div style="display:flex;justify-content:space-between;margin:6px 0;"><span>' + escapeHtml(s.name || '-') + '</span><span style="color:' + CHART_THEME.accent + ';">风险 ' + p[0] + ' / 收益 ' + p[1] + '</span></div>';
      })
      .join('');
    return '<div class="chart-box"><h3 style="margin:0 0 8px 0;">风险-收益散点</h3>' + rows + '</div>';
  }

  function renderHeatmapChart(heatmap) {
    const cats = (heatmap && heatmap.categories) || [];
    const values = (heatmap && heatmap.values) || [];
    if (!cats.length || !values.length) return renderNoDataPlaceholder('热力图', '暂无热力分布数据');
    const maxVal = Math.max.apply(null, values.map(function (v) { return Number(v) || 0; }));
    const cells = cats
      .map(function (c, i) {
        const v = Number(values[i]) || 0;
        const alpha = maxVal > 0 ? Math.max(0.12, v / maxVal) : 0.12;
        return '<div style="display:inline-block;padding:6px 8px;margin:4px;border-radius:6px;background:rgba(239,68,68,' + alpha.toFixed(2) + ');">' + escapeHtml(c) + ' (' + v + ')</div>';
      })
      .join('');
    return '<div class="chart-box"><h3 style="margin:0 0 8px 0;">日历热力</h3>' + cells + '</div>';
  }

  function renderWordcloud(items) {
    const arr = Array.isArray(items) ? items : [];
    if (!arr.length) return renderNoDataPlaceholder('词云', '暂无关键词数据');
    const tags = arr
      .map(function (t) {
        const w = Number(t.weight) || 10;
        const size = Math.max(12, Math.min(28, 10 + Math.round(w / 2)));
        return '<span style="display:inline-block;margin:6px 8px;font-size:' + size + 'px;color:' + CHART_THEME.secondary + ';">' + escapeHtml(t.text || '-') + '</span>';
      })
      .join('');
    return '<div class="chart-box"><h3 style="margin:0 0 8px 0;">关键词词云</h3>' + tags + '</div>';
  }

  function renderStackedBarChart(stacked) {
    const categories = (stacked && stacked.categories) || [];
    const series = (stacked && stacked.series) || [];
    if (!categories.length || !series.length) return renderNoDataPlaceholder('堆积柱状图', '暂无案件分布数据');
    const rows = categories
      .map(function (cat, idx) {
        const vals = series.map(function (s) { return Number((s.data || [])[idx]) || 0; });
        const total = vals.reduce(function (a, b) { return a + b; }, 0) || 1;
        const segments = vals
          .map(function (v, i) {
            const colors = [CHART_THEME.primary, CHART_THEME.danger, CHART_THEME.accent, CHART_THEME.secondary];
            const w = Math.max(5, Math.round((v / total) * 100));
            return '<span style="display:inline-block;height:100%;width:' + w + '%;background:' + colors[i % colors.length] + ';"></span>';
          })
          .join('');
        return '<div style="margin:10px 0;"><div style="display:flex;justify-content:space-between;font-size:12px;"><span>' + escapeHtml(cat) + '</span><span>' + total + '</span></div><div style="height:10px;background:rgba(255,255,255,.08);border-radius:99px;overflow:hidden;">' + segments + '</div></div>';
      })
      .join('');
    return '<div class="chart-box"><h3 style="margin:0 0 8px 0;">司法案件结构</h3>' + rows + '</div>';
  }

  function finishLoading(answer, charts) {
    renderCharts(charts, answer);
  }

  let isComposing = false;
  input.addEventListener('compositionstart', function () {
    isComposing = true;
  });
  input.addEventListener('compositionend', function () {
    isComposing = false;
  });

  function extractStockCodeAndYear(question) {
    const yearMatch = String(question).match(/(20\d{2})/);
    const year = yearMatch ? Number(yearMatch[1]) : 2022;
    const map = {
      比亚迪: '比亚迪',
      宁德时代: '宁德时代',
      蔚来: '蔚来',
      上汽集团: '上汽集团',
      长城汽车: '长城汽车',
      特斯拉: '特斯拉',
    };
    let stockCode = '比亚迪';
    Object.keys(map).forEach(function (k) {
      if (question.indexOf(k) >= 0) stockCode = map[k];
    });
    return { stockCode: stockCode, year: year };
  }

  function renderScoringCard(scoring) {
    if (!scoring) return;
    const ds = scoring.dimension_scores || {};
    const lines = Object.keys(ds)
      .map(function (k) {
        return k + ': ' + ds[k];
      })
      .join('\n');
    append(
      'bot',
      [
        '评分结果',
        '企业：' + safeText(scoring.stock_name, scoring.stock_code),
        '年份：' + safeText(scoring.year),
        '总分：' + safeText(scoring.total_score),
        '评级：' + safeText(scoring.rating),
        '维度：\n' + (lines || '-'),
      ].join('\n')
    );
    renderCharts(
      {
        gauge: { value: Number(scoring.total_score) || 0 },
      },
      { key_findings: ['评级：' + safeText(scoring.rating)] }
    );
  }

  function renderReportMessage(result) {
    const report = result.report || {};
    // Keep dialogue concise: only show final summary.
    append('bot', safeText(report.summary, '暂无摘要'));
  }

  function metricDisplayName(metric) {
    const m = String(metric || '').toLowerCase();
    const map = {
      sales_volume: '销量',
      revenue: '营收',
      net_profit: '净利润',
      total_assets: '总资产',
      roe: 'ROE',
    };
    return map[m] || safeText(metric, '指标');
  }

  function metricDisplayUnit(metric) {
    const m = String(metric || '').toLowerCase();
    if (m === 'sales_volume') return '辆';
    return '';
  }

  function buildFastPathMetricText(charts) {
    const ms = charts && charts.metric_series;
    if (!ms) return '';
    const categories = Array.isArray(ms.categories) ? ms.categories : [];
    const s0 = (ms.series && ms.series[0]) || {};
    const data = Array.isArray(s0.data) ? s0.data : [];
    const enterprise = safeText(ms.enterprise || s0.name, '该企业');
    const metric = metricDisplayName(ms.metric);
    const unit = metricDisplayUnit(ms.metric);
    const points = [];
    for (let i = 0; i < Math.min(categories.length, data.length); i += 1) {
      const v = data[i];
      if (v === null || v === undefined || Number.isNaN(Number(v))) continue;
      points.push({ year: String(categories[i]), value: Number(v) });
    }
    if (!points.length) return '';
    if (points.length === 1) {
      return enterprise + points[0].year + '年' + metric + '为 ' + points[0].value.toLocaleString() + (unit ? ' ' + unit : '');
    }
    const start = points[0];
    const end = points[points.length - 1];
    const rising = end.value > start.value;
    const falling = end.value < start.value;
    const trend = rising ? '呈逐年上升趋势' : falling ? '呈下降趋势' : '整体较为平稳';
    return (
      enterprise +
      metric +
      '从' +
      start.year +
      '年的' +
      start.value.toLocaleString() +
      '变化到' +
      end.year +
      '年的' +
      end.value.toLocaleString() +
      (unit ? unit : '') +
      '，' +
      trend +
      '。'
    );
  }

  function sendMsg() {
    const question = input.value.trim();
    if (!question || isSending) return;
    if (question.length < 3) {
      agentError.textContent = '问题至少需要 3 个字符';
      return;
    }
    agentError.textContent = '';
    agentStatus.textContent = '正在分析...';
    append('user', question);
    input.value = '';
    sendBtn.disabled = true;
    isSending = true;
    const requestId = ++activeRequestId;
    startFlowProgress();

    prepareForNewQuery(function onReadyForRequest() {
      if (requestId !== activeRequestId) {
        sendBtn.disabled = false;
        isSending = false;
        return;
      }
      analysisStartTime = Date.now();
      var task;
      if (question.indexOf('评分') >= 0) {
        const parsed = extractStockCodeAndYear(question);
        task = window.apiClient.getScoring(parsed.stockCode, parsed.year).then(function (scoring) {
          if (requestId !== activeRequestId) return;
          renderScoringCard(scoring);
          agentStatus.textContent = '评分完成';
          finishFlowProgress('分析完成').catch(function () {});
          schedulePanelReveal(requestId);
        });
      } else {
        task = window.apiClient.postAgentQuery(question, currentSessionId || null).then(function (result) {
          if (requestId !== activeRequestId) return;
          handleAgentQueryResult(result, requestId);
        });
      }

      task.catch(function (e) {
          finishFlowProgress('分析中断').catch(function () {});
          append('sys', '执行失败：' + e.message);
          agentError.textContent = '执行失败：' + e.message;
          showToast(e.message, 'error');
          agentStatus.textContent = '';
          if (chartPanel) {
            chartPanel.classList.remove('show');
          }
          localStorage.setItem('chartPanelCollapsed', '1');
          syncChartButtons();
          if (galaxy) galaxy.setIdle();
        })
        .finally(function () {
          sendBtn.disabled = false;
          isSending = false;
        });
    });
  }

  sendBtn.addEventListener('click', sendMsg);
  input.addEventListener('keydown', function (e) {
    if (e.key === 'Enter' && !isComposing) sendMsg();
  });

  newSessionBtn.addEventListener('click', function () {
    stopFlowProgressInstant();
    currentSessionId = '';
    sourcePanel.textContent = '暂无证据';
    btnEvidence.textContent = '证据';
    append('sys', '已新建会话（前端上下文已清空）。');
    agentStatus.textContent = '已新建会话，请输入新问题';
    agentError.textContent = '';
    chartPanel.classList.remove('show');
    localStorage.setItem('chartPanelCollapsed', '1');
    syncChartButtons();
    if (galaxy) galaxy.setIdle();
  });

  logoutBtn.addEventListener('click', function () {
    window.apiClient.clearToken();
    window.location.href = './login.html';
  });

  btnEvidence.addEventListener('click', function (e) {
    e.stopPropagation();
    evidencePanel.classList.toggle('open');
  });
  document.addEventListener('click', function (e) {
    if (!evidencePanel.contains(e.target) && e.target !== btnEvidence) evidencePanel.classList.remove('open');
  });
  refreshSourcesBtn.addEventListener('click', function () {
    agentError.textContent = '';
    refreshSessionSources()
      .then(function () {
        agentStatus.textContent = '会话证据已刷新';
      })
      .catch(function (e) {
        agentError.textContent = '刷新证据失败：' + e.message;
      });
  });

  toggleChartBtn.addEventListener('click', function () {
    if (!chartPanel) return;
    setChartCollapsed(!isChartCollapsed(), true);
  });
  collapseChartBtn.addEventListener('click', function () {
    if (!chartPanel) return;
    setChartCollapsed(!isChartCollapsed(), true);
  });
  if (chartPanel) {
    chartPanel.addEventListener('dblclick', function (e) {
      if (e.target.closest('button, .chart-tools, .chart-head')) return;
      if (!chartPanel.classList.contains('show')) return;
      setChartCollapsed(true, true);
    });
  }

  if (localStorage.getItem('chartPanelCollapsed') === null) {
    localStorage.setItem('chartPanelCollapsed', '1');
  }
  if (chartPanel) {
    if (isChartCollapsed()) {
      chartPanel.classList.remove('show');
      if (galaxy) galaxy.setIdle();
    } else {
      chartPanel.classList.add('show');
      if (galaxy) galaxy.hideForPanelOpen();
    }
  }
  userCollapsedChartThisSession = false;
  syncChartButtons();
  function uploadFile(file) {
    if (!file) return;
    if (!agentStatus) {
      console.warn('agentStatus 不存在，跳过上传状态展示');
    }
    if (agentStatus) agentStatus.textContent = '文件上传中...';
    return window.apiClient
      .uploadFile(file, currentSessionId || null)
      .then(function (data) {
        currentSessionId = data.session_id || currentSessionId;
        const text = '已上传文件：' + (data.filename || file.name) + '，请告诉我你想分析的内容。';
        input.value = text;
        append('sys', text);
        if (agentStatus) agentStatus.textContent = '上传完成';
      })
      .catch(function (e) {
        if (agentError) agentError.textContent = '上传失败：' + e.message;
      });
  }

  if (uploadBtn && fileInput) {
    uploadBtn.addEventListener('click', function () {
      fileInput.click();
    });
    fileInput.addEventListener('change', function (e) {
      const f = e.target.files && e.target.files[0];
      uploadFile(f);
    });
  } else {
    console.warn('uploadBtn 或 fileInput 不存在，上传功能未挂载');
  }

  if (chatInputWrap) {
    ['dragenter', 'dragover'].forEach(function (ev) {
      chatInputWrap.addEventListener(ev, function (e) {
        e.preventDefault();
        e.stopPropagation();
        chatInputWrap.classList.add('dragover');
      });
    });
    ['dragleave', 'drop'].forEach(function (ev) {
      chatInputWrap.addEventListener(ev, function (e) {
        e.preventDefault();
        e.stopPropagation();
        chatInputWrap.classList.remove('dragover');
      });
    });
    chatInputWrap.addEventListener('drop', function (e) {
      const f = e.dataTransfer && e.dataTransfer.files && e.dataTransfer.files[0];
      uploadFile(f);
    });
  } else {
    console.warn('chat-input 容器不存在，拖拽上传未挂载');
  }

  if (downloadPdfBtn) {
    downloadPdfBtn.addEventListener('click', function () {
      if (!currentSessionId) return;
      window.open('/api/v1/report/download/' + encodeURIComponent(currentSessionId), '_blank');
    });
  } else {
    console.warn('downloadPdfBtn 不存在，PDF下载按钮未挂载');
  }

  // Keep chart rendering entrypoints request-driven (API response handlers only).

  function initMe() {
    window.apiClient
      .getMe()
      .then(function (me) {
        if (currentUserEl) currentUserEl.textContent = safeText(me.email, '已登录');
      })
      .catch(function () {
        window.apiClient.clearToken();
        window.location.replace('./login.html');
      });
  }
  window.addEventListener('offline', function () {
    showToast('网络已断开，请检查连接', 'error');
  });
  window.addEventListener('online', function () {
    showToast('网络已恢复', 'info');
  });
  window.addEventListener('resize', function () {
    adjustChartLayout();
  });
  window.addEventListener('keydown', function (evt) {
    if (evt.key === 'Escape' && fullscreenCard) {
      fullscreenCard.classList.remove('chart-card-fullscreen');
      resizeCardChart(fullscreenCard);
      fullscreenCard = null;
    }
  });
  initMe();
  append('sys', '欢迎使用，请直接输入企业问题。');
})();
