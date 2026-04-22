(function () {
  'use strict';

  var canvas = document.getElementById('bg');
  var ctx = canvas && canvas.getContext ? canvas.getContext('2d') : null;
  var errorEl = document.getElementById('error');
  var successEl = document.getElementById('success');
  var loginBtn = document.getElementById('loginBtn');
  var registerBtn = document.getElementById('registerBtn');
  var modeLoginBtn = document.getElementById('modeLogin');
  var modeRegisterBtn = document.getElementById('modeRegister');
  var quickEntry = document.getElementById('quickEntry');
  var usernameEl = document.getElementById('username');
  var passwordEl = document.getElementById('password');
  var usernameLabel = document.getElementById('usernameLabel');
  var passwordLabel = document.getElementById('passwordLabel');
  var usernameHint = document.getElementById('usernameHint');
  var passwordHint = document.getElementById('passwordHint');

  var mode = 'login'; // 'login' | 'register'

  if (localStorage.getItem('token') && quickEntry) quickEntry.style.display = 'block';

  function showError(msg) {
    if (successEl) successEl.style.display = 'none';
    if (errorEl) {
      errorEl.textContent = msg;
      errorEl.style.display = 'block';
    }
  }

  window.addEventListener('error', function (e) {
    try {
      var m = (e && (e.message || (e.error && e.error.message))) || '页面脚本异常';
      showError('脚本错误：' + m);
    } catch (_) {}
  });

  function showSuccess(msg) {
    if (errorEl) errorEl.style.display = 'none';
    if (successEl) {
      successEl.textContent = msg;
      successEl.style.display = 'block';
    }
  }

  function clearMsgs() {
    if (errorEl) {
      errorEl.style.display = 'none';
      errorEl.textContent = '';
    }
    if (successEl) {
      successEl.style.display = 'none';
      successEl.textContent = '';
    }
  }

  function validate() {
    var username = (usernameEl && usernameEl.value || '').trim();
    var password = (passwordEl && passwordEl.value || '').trim();
    if (!username || !password) {
      showError('用户名和密码不能为空');
      return null;
    }
    if (password.length < 8) {
      showError('密码至少 8 位（例如：DemoPass123）');
      return null;
    }
    return { username: username, password: password };
  }

  function setMode(next) {
    mode = next === 'register' ? 'register' : 'login';
    clearMsgs();
    if (modeLoginBtn) modeLoginBtn.classList.toggle('active', mode === 'login');
    if (modeRegisterBtn) modeRegisterBtn.classList.toggle('active', mode === 'register');
    if (loginBtn) loginBtn.style.display = mode === 'login' ? 'block' : 'none';
    if (registerBtn) registerBtn.style.display = mode === 'register' ? 'block' : 'none';

    if (usernameLabel) usernameLabel.textContent = mode === 'register' ? '注册账号' : '账号';
    if (passwordLabel) passwordLabel.textContent = mode === 'register' ? '设置密码' : '密码';
    if (usernameEl) usernameEl.placeholder = '请输入账号（如 123 / user@xx.com）';
    if (passwordEl) passwordEl.placeholder = '请输入密码（至少 8 位，如 DemoPass123）';

    if (usernameHint) {
      usernameHint.textContent =
        '提示：不含 @ 的账号会自动补全为邮箱（例如输入 123 → 123@example.com）';
    }
    if (passwordHint) {
      passwordHint.textContent =
        mode === 'register'
          ? '注册要求：密码至少 8 位；建议包含大小写字母与数字。'
          : '提示：忘记密码请重新注册一个新账号（演示环境）。';
    }
  }

  async function handleLogin() {
    var payload = validate();
    if (!payload) return;
    clearMsgs();
    loginBtn.disabled = true;
    try {
      showSuccess('正在登录...');
      await window.apiClient.login(payload.username, payload.password);
      showSuccess('登录成功，正在进入系统...');
      setTimeout(function () {
        window.location.href = './index.html';
      }, 500);
    } catch (e) {
      showError(e.message || '登录失败，请重试');
    } finally {
      loginBtn.disabled = false;
    }
  }

  async function handleRegister() {
    var payload = validate();
    if (!payload) return;
    clearMsgs();
    registerBtn.disabled = true;
    try {
      showSuccess('正在注册...');
      await window.apiClient.register(payload.username, payload.password);
      await window.apiClient.login(payload.username, payload.password);
      showSuccess('注册并登录成功，正在进入系统...');
      setTimeout(function () {
        window.location.href = './index.html';
      }, 600);
    } catch (e) {
      showError(e.message || '注册失败，请检查输入');
    } finally {
      registerBtn.disabled = false;
    }
  }

  function initStarBackground() {
    if (!canvas || !ctx) return;
    function resizeCanvas() {
      canvas.width = window.innerWidth;
      canvas.height = window.innerHeight;
    }
    resizeCanvas();
    window.addEventListener('resize', resizeCanvas);

    var stars = [];
    function initStars() {
      stars = [];
      for (var i = 0; i < 600; i += 1) {
        stars.push({
          x: Math.random() * canvas.width,
          y: Math.random() * canvas.height,
          alpha: Math.random(),
          speed: Math.random() * 0.2,
        });
      }
    }
    initStars();
    window.addEventListener('resize', initStars);

    function animate() {
      ctx.fillStyle = '#000';
      ctx.fillRect(0, 0, canvas.width, canvas.height);
      for (var j = 0; j < stars.length; j += 1) {
        var s = stars[j];
        s.y += s.speed;
        if (s.y > canvas.height) s.y = 0;
        ctx.fillStyle = 'rgba(255,255,255,' + s.alpha + ')';
        ctx.fillRect(s.x, s.y, 1, 1);
      }
      requestAnimationFrame(animate);
    }
    animate();
  }

  if (loginBtn) loginBtn.addEventListener('click', handleLogin);
  if (registerBtn) registerBtn.addEventListener('click', handleRegister);
  if (modeLoginBtn) modeLoginBtn.addEventListener('click', function () { setMode('login'); });
  if (modeRegisterBtn) modeRegisterBtn.addEventListener('click', function () { setMode('register'); });
  document.addEventListener('keydown', function (e) {
    if (e.key === 'Enter') {
      if (mode === 'register') handleRegister();
      else handleLogin();
    }
  });
  setMode('login');
  initStarBackground();
})();

