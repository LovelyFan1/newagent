/* global THREE */
(function () {
  'use strict';

  function clamp(n, min, max) {
    return Math.max(min, Math.min(max, n));
  }

  function prefersReducedMotion() {
    try {
      return window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
    } catch (_) {
      return false;
    }
  }

  function isMobileLike() {
    return (
      window.innerWidth <= 900 ||
      /Mobi|Android|iPhone|iPad|iPod|HarmonyOS|Windows Phone/i.test(navigator.userAgent || '')
    );
  }

  function pickQuality() {
    const reduced = prefersReducedMotion();
    // 兼容：部分环境默认开启“减少动态效果”，会导致登录后土星不转。
    // 这里改为低粒子量 + 仍然动画旋转（保证“土星在转”的观感稳定）。
    if (reduced) return { factor: 0.22, animate: true, reduced: false };
    const cores = typeof navigator.hardwareConcurrency === 'number' ? navigator.hardwareConcurrency : 4;
    const dpr = typeof window.devicePixelRatio === 'number' ? window.devicePixelRatio : 1;
    let factor = 1.0;
    if (isMobileLike()) factor *= 0.55;
    if (dpr >= 2) factor *= 0.75;
    if (cores <= 4) factor *= 0.75;
    return { factor: clamp(factor, 0.35, 1.0), animate: true, reduced: false };
  }

  function createGalaxyBackground() {
    const canvas = document.createElement('canvas');
    canvas.width = 2048;
    canvas.height = 1024;
    const ctx = canvas.getContext('2d');
    const grad = ctx.createRadialGradient(1100, 520, 60, 1100, 520, 1200);
    grad.addColorStop(0, '#0a0a12');
    grad.addColorStop(0.45, '#040409');
    grad.addColorStop(1, '#000000');
    ctx.fillStyle = grad;
    ctx.fillRect(0, 0, canvas.width, canvas.height);
    for (let i = 0; i < 4500; i++) {
      const x = Math.random() * canvas.width;
      const y = Math.random() * canvas.height;
      const a = Math.random() * 0.035;
      ctx.fillStyle = 'rgba(255,220,160,' + a.toFixed(4) + ')';
      ctx.fillRect(x, y, 2, 2);
    }
    for (let i = 0; i < 2600; i++) {
      const x = Math.random() * canvas.width;
      const y = Math.random() * canvas.height;
      const alpha = 0.10 + Math.random() * 0.35;
      ctx.fillStyle = 'rgba(255,255,255,' + alpha.toFixed(4) + ')';
      ctx.fillRect(x, y, 1, 1);
    }
    for (let i = 0; i < 260; i++) {
      const x = Math.random() * canvas.width;
      const y = Math.random() * canvas.height;
      const size = 0.6 + Math.random() * 1.8;
      const warm = Math.random() < 0.22;
      const alpha = 0.35 + Math.random() * 0.55;
      ctx.fillStyle = warm
        ? 'rgba(255,240,200,' + alpha.toFixed(4) + ')'
        : 'rgba(255,255,255,' + alpha.toFixed(4) + ')';
      ctx.beginPath();
      ctx.arc(x, y, size, 0, Math.PI * 2);
      ctx.fill();
    }
    return { texture: new THREE.CanvasTexture(canvas) };
  }

  function createParticleTexture() {
    const canvas = document.createElement('canvas');
    canvas.width = 128;
    canvas.height = 128;
    const ctx = canvas.getContext('2d');
    // 更接近你“原始版本”的柔和光点：中心亮、边缘暖色渐隐
    const g = ctx.createRadialGradient(64, 64, 0, 64, 64, 64);
    g.addColorStop(0, 'rgba(255,255,255,1)');
    g.addColorStop(0.2, 'rgba(255,240,200,0.95)');
    g.addColorStop(0.4, 'rgba(255,210,140,0.6)');
    g.addColorStop(1, 'rgba(255,200,120,0)');
    ctx.fillStyle = g;
    ctx.fillRect(0, 0, 128, 128);
    return new THREE.CanvasTexture(canvas);
  }

  function createMovingStars(scene, factor, texture) {
    // 轻微流动星空：更细小、更柔和，避免“大像素点”
    const count = Math.max(1800, Math.floor(4200 * factor));
    const pos = new Float32Array(count * 3);
    for (let i = 0; i < count; i++) {
      pos[i * 3] = (Math.random() - 0.5) * 200;
      pos[i * 3 + 1] = (Math.random() - 0.5) * 200;
      pos[i * 3 + 2] = -Math.random() * 200;
    }
    const geo = new THREE.BufferGeometry();
    geo.setAttribute('position', new THREE.BufferAttribute(pos, 3));
    const mat = new THREE.PointsMaterial({
      size: 0.22,
      map: texture || null,
      color: 0xffffff,
      transparent: true,
      opacity: 0.22,
      blending: THREE.AdditiveBlending,
      depthWrite: false,
    });
    const stars = new THREE.Points(geo, mat);
    scene.add(stars);
    return stars;
  }

  var COLLAPSE_MS = 800;
  var APPEAR_MS = 800;
  // idle / hidden / 动画 共用：自转基速（_animateFrame 中按 appState 放大）
  var SPIN_PLANET_IDLE = 0.004;
  var SPIN_RING_IDLE = 0.0012;
  var SPIN_STAR_IDLE = 0.0003;
  var SPIN_MULT_ANALYZE = 4.5;

  function Galaxy() {
    this.scene = null;
    this.camera = null;
    this.renderer = null;
    this.rootGroup = null;
    this.planet = null;
    this.ring = null;
    this.ringGlow = null;
    this.starField = null;
    this.isDown = false;
    this.rotX = 0.35;
    this.rotY = 0.6;
    // idle | analyzing | collapsing | appearing | hidden
    this.appState = 'idle';
    this._collapseCallback = null;
    this._appearCallback = null;
    this._animStart = 0;
    this._animDuration = 800;
    this.currentScale = 1;
    this.fade = 1;
    this._raf = null;
    this._running = false;
    this._reduced = false;
  }

  Galaxy.prototype.init = function init() {
    if (!window.THREE) throw new Error('THREE 未加载');
    const q = pickQuality();
    this._reduced = q.reduced;
    this.scene = new THREE.Scene();
    this.scene.background = createGalaxyBackground().texture;
    this.camera = new THREE.PerspectiveCamera(60, innerWidth / innerHeight, 0.1, 1000);
    // 回滚到你最初版的经典视角
    this.camera.position.set(0, 0.8, 6);
    this.renderer = new THREE.WebGLRenderer({ antialias: true });
    this.renderer.setSize(innerWidth, innerHeight);
    const dpr = typeof window.devicePixelRatio === 'number' ? window.devicePixelRatio : 1;
    this.renderer.setPixelRatio(clamp(dpr, 1, 1.6));
    // 兜底：确保 canvas 在背景层且不遮挡交互
    const el = this.renderer.domElement;
    el.id = 'galaxyCanvas';
    el.style.position = 'fixed';
    el.style.left = '0';
    el.style.top = '0';
    el.style.width = '100%';
    el.style.height = '100%';
    el.style.zIndex = '5';
    el.style.pointerEvents = 'none';
    document.body.appendChild(el);
    this.rootGroup = new THREE.Group();
    // 回滚到你最初版的位置（更像“土星在左侧”）
    this.rootGroup.position.set(-1.2, 0.8, 0);
    this.scene.add(this.rootGroup);
    const texture = createParticleTexture();
    const factor = q.factor;
    // 回滚到最初版的粒子规模（更干净、更好看）
    const planetCount = Math.floor(20000 * factor);
    const ringCount = Math.floor(30000 * factor);

    // 轻微流动星空层（不属于 rootGroup）
    this.starField = createMovingStars(this.scene, factor, texture);

    if (planetCount > 0) {
      const pos = new Float32Array(planetCount * 3);
      for (let i = 0; i < planetCount; i++) {
        const u = Math.random();
        const v = Math.random();
        const theta = 2 * Math.PI * u;
        const phi = Math.acos(2 * v - 1);
        const r = 1.18 + Math.random() * 0.04;
        pos[i * 3] = r * Math.sin(phi) * Math.cos(theta);
        pos[i * 3 + 1] = r * Math.sin(phi) * Math.sin(theta);
        pos[i * 3 + 2] = r * Math.cos(phi);
      }
      const geo = new THREE.BufferGeometry();
      geo.setAttribute('position', new THREE.BufferAttribute(pos, 3));
      const mat = new THREE.PointsMaterial({
        // 回滚到最初版：更大更柔的粒子
        size: 0.04,
        map: texture,
        color: 0xffd7a8,
        transparent: true,
        opacity: 0.95,
        blending: THREE.AdditiveBlending,
        depthWrite: false,
      });
      this.planet = new THREE.Points(geo, mat);
      this.rootGroup.add(this.planet);
    }

    if (ringCount > 0) {
      const rpos = new Float32Array(ringCount * 3);
      for (let i = 0; i < ringCount; i++) {
        const a = Math.random() * Math.PI * 2;
        const radius = 1.7 + Math.pow(Math.random(), 1.4) * 1.8;
        rpos[i * 3] = Math.cos(a) * radius;
        rpos[i * 3 + 1] = (Math.random() - 0.5) * 0.06;
        rpos[i * 3 + 2] = Math.sin(a) * radius;
      }
      const rg = new THREE.BufferGeometry();
      rg.setAttribute('position', new THREE.BufferAttribute(rpos, 3));
      this.ring = new THREE.Points(
        rg,
        new THREE.PointsMaterial({
          size: 0.025,
          map: texture,
          color: 0xf5d6a0,
          transparent: true,
          opacity: 0.65,
          blending: THREE.AdditiveBlending,
          depthWrite: false,
        })
      );
      this.ring.rotation.x = 0.55;
      this.rootGroup.add(this.ring);

      // 发光层：和你原始版本一致，让“环带”更容易被看见
      this.ringGlow = new THREE.Points(
        rg,
        new THREE.PointsMaterial({
          size: 0.05,
          map: texture,
          transparent: true,
          opacity: 0.25,
          blending: THREE.AdditiveBlending,
          depthWrite: false,
        })
      );
      this.ringGlow.rotation.x = 0.55;
      this.rootGroup.add(this.ringGlow);
    }

    window.addEventListener('mousedown', () => { this.isDown = true; });
    window.addEventListener('mouseup', () => { this.isDown = false; });
    window.addEventListener('mousemove', (e) => { if (!this.isDown) return; this.rotY += (e.movementX || 0) * 0.005; this.rotX += (e.movementY || 0) * 0.005; });
    window.addEventListener('resize', () => {
      this.camera.aspect = innerWidth / innerHeight;
      this.camera.updateProjectionMatrix();
      this.renderer.setSize(innerWidth, innerHeight);
    });
    document.addEventListener('visibilitychange', () => {
      if (document.hidden) this.stop();
      else this.start();
    });
    if (q.animate) this.start();
    else this.renderOnce();
  };

  Galaxy.prototype.renderOnce = function renderOnce() {
    if (!this.renderer) return;
    this.rootGroup.rotation.x = this.rotX;
    this.rootGroup.rotation.y = this.rotY;
    this.renderer.render(this.scene, this.camera);
  };

  Galaxy.prototype.start = function start() {
    if (this._running || this._reduced) return;
    this._running = true;
    const tick = () => {
      if (!this._running) return;
      this._raf = requestAnimationFrame(tick);
      this._animateFrame();
    };
    this._raf = requestAnimationFrame(tick);
  };

  Galaxy.prototype.stop = function stop() {
    this._running = false;
    if (this._raf) cancelAnimationFrame(this._raf);
    this._raf = null;
  };

  Galaxy.prototype._applyGroupFade = function (t) {
    this.fade = t;
    if (this.planet && this.planet.material) this.planet.material.opacity = 0.95 * t;
    if (this.ring && this.ring.material) this.ring.material.opacity = 0.65 * t;
    if (this.ringGlow && this.ringGlow.material) this.ringGlow.material.opacity = 0.25 * t;
  };

  Galaxy.prototype.getAppState = function () {
    return this.appState;
  };

  /** 立即回到空闲态（不播放动画）：用于错误 / 需补充信息等 */
  Galaxy.prototype.setIdle = function setIdle() {
    this._collapseCallback = null;
    this._appearCallback = null;
    this.appState = 'idle';
    this.currentScale = 1;
    this._applyGroupFade(1);
    if (this.rootGroup) this.rootGroup.scale.set(1, 1, 1);
    this._setCanvasClass(false);
  };

  /** 分析中：自转明显加速，保持完整尺度 */
  Galaxy.prototype.setAnalyzing = function setAnalyzing() {
    this._collapseCallback = null;
    this._appearCallback = null;
    this.appState = 'analyzing';
    this.currentScale = 1;
    this._applyGroupFade(1);
    if (this.rootGroup) this.rootGroup.scale.set(1, 1, 1);
    this._setCanvasClass(true);
  };

  /** 手动展开左侧大屏时：立即隐藏土星（不播放 0.8s 坍缩，避免与交互打架） */
  Galaxy.prototype.hideForPanelOpen = function hideForPanelOpen() {
    this._collapseCallback = null;
    this._appearCallback = null;
    this.appState = 'hidden';
    this.currentScale = 0;
    this._applyGroupFade(0);
    if (this.rootGroup) this.rootGroup.scale.set(0.0001, 0.0001, 0.0001);
    this._setCanvasClass(false);
  };

  /** 坍缩：固定 800ms 完整播完，再回调 */
  Galaxy.prototype.playCollapse = function playCollapse(onComplete) {
    if (this.appState === 'hidden' && this.currentScale < 0.02) {
      this._setCanvasClass(false);
      if (typeof onComplete === 'function') {
        setTimeout(function () {
          onComplete();
        }, 0);
      }
      return;
    }
    this._collapseCallback = typeof onComplete === 'function' ? onComplete : null;
    this._appearCallback = null;
    this.appState = 'collapsing';
    this._animStart = typeof performance !== 'undefined' && performance.now ? performance.now() : Date.now();
    this._animDuration = COLLAPSE_MS;
    this._setCanvasClass(true);
    if (!this._running) this.start();
  };

  /** 自隐藏状态回到空闲；若已在空闲则尽快回调 */
  Galaxy.prototype.playAppear = function playAppear(onComplete) {
    var done = typeof onComplete === 'function' ? onComplete : null;
    this._collapseCallback = null;
    this._setCanvasClass(false);
    if (this.appState === 'idle' && this.currentScale > 0.92 && this.fade > 0.9) {
      if (done) setTimeout(done, 0);
      return;
    }
    this._appearCallback = done;
    this.appState = 'appearing';
    this._animStart = typeof performance !== 'undefined' && performance.now ? performance.now() : Date.now();
    this._animDuration = APPEAR_MS;
    this.currentScale = 0.0001;
    this._applyGroupFade(0);
    if (this.rootGroup) this.rootGroup.scale.set(0.0001, 0.0001, 0.0001);
    if (!this._running) this.start();
  };

  Galaxy.prototype._setCanvasClass = function (analyzing) {
    var el = this.renderer && this.renderer.domElement;
    if (!el) return;
    if (analyzing) el.classList.add('galaxy-analyzing');
    else el.classList.remove('galaxy-analyzing');
  };

  /** @deprecated 由 app 使用 setIdle / setAnalyzing / playAppear */
  Galaxy.prototype.restoreVisuals = function restoreVisuals() {
    this.setIdle();
  };
  /** @deprecated */
  Galaxy.prototype.setLoading = function setLoading() {
    this.setAnalyzing();
  };
  /** @deprecated */
  Galaxy.prototype.setDone = function setDone() {
    this.appState = 'hidden';
    this.currentScale = 0;
    this._applyGroupFade(0);
    if (this.rootGroup) this.rootGroup.scale.set(0.0001, 0.0001, 0.0001);
  };

  Galaxy.prototype._easingCubicIn = function (t) {
    return t * t * t;
  };
  Galaxy.prototype._easingCubicOut = function (t) {
    return 1 - Math.pow(1 - t, 3);
  };

  Galaxy.prototype._animateFrame = function _animateFrame() {
    if (!this.rootGroup) return;
    this.rootGroup.rotation.x = this.rotX;
    this.rootGroup.rotation.y = this.rotY;

    var mult = 1;
    if (this.appState === 'analyzing' || this.appState === 'collapsing') {
      mult = SPIN_MULT_ANALYZE;
    }

    var now = typeof performance !== 'undefined' && performance.now ? performance.now() : Date.now();
    if (this.appState === 'collapsing') {
      var tc = (now - this._animStart) / this._animDuration;
      if (tc >= 1) {
        this.currentScale = 0;
        this._applyGroupFade(0);
        this.rootGroup.scale.set(0.0001, 0.0001, 0.0001);
        this.appState = 'hidden';
        this._setCanvasClass(false);
        var cb0 = this._collapseCallback;
        this._collapseCallback = null;
        if (cb0) {
          try {
            cb0();
          } catch (e) {}
        }
      } else {
        var eIn = this._easingCubicIn(tc);
        this.currentScale = 1 - eIn;
        this._applyGroupFade(1 - tc);
        var sc = Math.max(0.0001, this.currentScale);
        this.rootGroup.scale.set(sc, sc, sc);
        if (this.planet) this.planet.rotation.y += SPIN_PLANET_IDLE * mult;
        if (this.ring) this.ring.rotation.y += SPIN_RING_IDLE * mult;
        if (this.ringGlow) this.ringGlow.rotation.y += SPIN_RING_IDLE * mult;
        if (this.starField) this.starField.rotation.y += SPIN_STAR_IDLE * (mult * 0.6);
      }
    } else if (this.appState === 'appearing') {
      var ta = (now - this._animStart) / this._animDuration;
      if (ta >= 1) {
        this.appState = 'idle';
        this.currentScale = 1;
        this._applyGroupFade(1);
        this.rootGroup.scale.set(1, 1, 1);
        var cb1 = this._appearCallback;
        this._appearCallback = null;
        if (cb1) {
          try {
            cb1();
          } catch (e) {}
        }
      } else {
        var eOut = this._easingCubicOut(ta);
        this.currentScale = eOut;
        this._applyGroupFade(ta);
        this.rootGroup.scale.set(eOut, eOut, eOut);
        if (this.planet) this.planet.rotation.y += SPIN_PLANET_IDLE;
        if (this.ring) this.ring.rotation.y += SPIN_RING_IDLE;
        if (this.ringGlow) this.ringGlow.rotation.y += SPIN_RING_IDLE;
        if (this.starField) this.starField.rotation.y += SPIN_STAR_IDLE;
      }
    } else {
      if (this.appState === 'analyzing') {
        if (this.planet) this.planet.rotation.y += SPIN_PLANET_IDLE * SPIN_MULT_ANALYZE;
        if (this.ring) this.ring.rotation.y += SPIN_RING_IDLE * SPIN_MULT_ANALYZE;
        if (this.ringGlow) this.ringGlow.rotation.y += SPIN_RING_IDLE * SPIN_MULT_ANALYZE;
        if (this.starField) this.starField.rotation.y += SPIN_STAR_IDLE * 1.4;
        this.currentScale = 1;
        this._applyGroupFade(1);
        this.rootGroup.scale.set(1, 1, 1);
      } else if (this.appState === 'hidden') {
        if (this.planet) this.planet.rotation.y += SPIN_PLANET_IDLE * 0.15;
        if (this.ring) this.ring.rotation.y += SPIN_RING_IDLE * 0.15;
        if (this.ringGlow) this.ringGlow.rotation.y += SPIN_RING_IDLE * 0.15;
      } else {
        if (this.planet) this.planet.rotation.y += SPIN_PLANET_IDLE;
        if (this.ring) this.ring.rotation.y += SPIN_RING_IDLE;
        if (this.ringGlow) this.ringGlow.rotation.y += SPIN_RING_IDLE;
        if (this.starField) this.starField.rotation.y += SPIN_STAR_IDLE;
        this.currentScale = 1;
        this._applyGroupFade(1);
        this.rootGroup.scale.set(1, 1, 1);
      }
    }
    this.renderer.render(this.scene, this.camera);
  };

  window.GalaxyBackground = {
    create: function () {
      const g = new Galaxy();
      g.init();
      // 若脚本已加载但初始化失败，提供可见提示（避免“看起来像没土星”）
      try {
        window.__GALAXY_READY__ = true;
      } catch (_) {}
      return g;
    },
  };
})();
