/* global window, localStorage */
(function () {
  'use strict';

  function defaultLoginPath() {
    if (window.location.pathname.indexOf('/web/') === 0) return '/web/login.html';
    return '/login.html';
  }

  function sleep(ms) {
    return new Promise(function (resolve) {
      setTimeout(resolve, ms);
    });
  }

  function ApiClient(config) {
    var cfg = config || {};
    this.baseUrl = typeof cfg.baseUrl === 'string' ? cfg.baseUrl : window.API_BASE_URL || '';
    this.maxRetries = Number.isFinite(Number(cfg.maxRetries)) ? Number(cfg.maxRetries) : 2;
    this.retryDelayMs = Number.isFinite(Number(cfg.retryDelayMs)) ? Number(cfg.retryDelayMs) : 1000;
    this.loginPath = cfg.loginPath || defaultLoginPath();
    this.tokenKey = 'token';
  }

  function normalizeEmail(username) {
    var u = String(username || '').trim();
    if (!u) return '';
    return u.indexOf('@') >= 0 ? u : u + '@example.com';
  }

  ApiClient.prototype.getToken = function () {
    return localStorage.getItem(this.tokenKey) || '';
  };

  ApiClient.prototype.setToken = function (token) {
    if (!token) return;
    localStorage.setItem(this.tokenKey, token);
  };

  ApiClient.prototype.clearToken = function () {
    localStorage.removeItem(this.tokenKey);
  };

  ApiClient.prototype._buildUrl = function (path) {
    if (!path) return this.baseUrl || '';
    if (this.baseUrl && path.charAt(0) === '/') return this.baseUrl + path;
    return path;
  };

  ApiClient.prototype._redirectToLogin = function () {
    if (window.location.pathname !== this.loginPath) {
      window.location.replace(this.loginPath);
    }
  };

  ApiClient.prototype._normalizeError = function (err) {
    if (!err) return '请求失败，请稍后重试';
    if (typeof err === 'string') return err;
    if (err.message) return err.message;
    return '请求失败，请稍后重试';
  };

  ApiClient.prototype._extractWrappedData = function (raw) {
    if (!raw || typeof raw !== 'object') throw new Error('服务返回格式异常');
    if (typeof raw.code !== 'number') return raw;
    if (raw.code !== 0) {
      // FastAPI validation errors are wrapped as: {code:422, data:[{msg:...}], message:"Validation Error"}
      if (raw.code === 422 && Array.isArray(raw.data) && raw.data.length) {
        var first = raw.data[0] || {};
        throw new Error(first.msg || raw.message || '参数校验失败');
      }
      throw new Error(raw.message || '服务处理失败');
    }
    return raw.data;
  };

  ApiClient.prototype._request = async function (path, options) {
    var opts = options || {};
    var method = (opts.method || 'GET').toUpperCase();
    var headers = new Headers(opts.headers || {});
    var token = this.getToken();
    if (token && !headers.has('Authorization')) {
      headers.set('Authorization', 'Bearer ' + token);
    }
    var body = opts.body;
    if (opts.json !== undefined) {
      headers.set('Content-Type', 'application/json');
      body = JSON.stringify(opts.json);
    }
    var attempt = 0;
    var url = this._buildUrl(path);
    while (attempt <= this.maxRetries) {
      try {
        var response = await fetch(url, {
          method: method,
          headers: headers,
          body: body,
        });
        var payload;
        try {
          payload = await response.json();
        } catch (_) {
          payload = {};
        }

        if (response.status === 401) {
          this.clearToken();
          this._redirectToLogin();
          throw new Error('登录已过期，请重新登录');
        }

        if (!response.ok) {
          var msg = payload && (payload.message || payload.detail || payload.error);
          throw new Error(msg || '请求失败（HTTP ' + response.status + '）');
        }

        return this._extractWrappedData(payload);
      } catch (err) {
        var isNetwork = err instanceof TypeError || !navigator.onLine;
        if (isNetwork && attempt < this.maxRetries) {
          attempt += 1;
          await sleep(this.retryDelayMs);
          continue;
        }
        throw new Error(this._normalizeError(err));
      }
    }
    throw new Error('请求失败，请稍后重试');
  };

  ApiClient.prototype.register = function (username, password) {
    var email = normalizeEmail(username);
    return this._request('/api/v1/auth/register', {
      method: 'POST',
      json: { email: email, password: password },
    });
  };

  ApiClient.prototype.login = function (username, password) {
    var form = new URLSearchParams();
    form.set('username', normalizeEmail(username));
    form.set('password', password);
    return this._request('/api/v1/auth/login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: form.toString(),
    }).then(
      function (data) {
        if (data && data.access_token) this.setToken(data.access_token);
        return data;
      }.bind(this)
    );
  };

  ApiClient.prototype.getMe = function () {
    return this._request('/api/v1/auth/me');
  };

  ApiClient.prototype.getScoring = function (stockCode, year) {
    var q = '?year=' + encodeURIComponent(String(year));
    return this._request('/api/v1/scoring/' + encodeURIComponent(stockCode) + q);
  };

  ApiClient.prototype.postAgentQuery = function (question, sessionId) {
    return this._request('/api/v1/agent/query', {
      method: 'POST',
      json: { question: question, session_id: sessionId || null },
    });
  };

  ApiClient.prototype.uploadFile = function (file, sessionId) {
    if (!file) return Promise.reject(new Error('未选择文件'));
    var form = new FormData();
    form.append('file', file);
    if (sessionId) form.append('session_id', sessionId);
    return this._request('/api/v1/files/upload', {
      method: 'POST',
      body: form,
    });
  };

  window.ApiClient = ApiClient;
  window.apiClient = new ApiClient();
})();

