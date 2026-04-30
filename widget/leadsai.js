(async function initLeadsAI() {
  const scriptTag = document.currentScript || document.getElementById("leadsai-embed-script");
  if (!scriptTag) {
    console.error("LeadsAI: Could not locate script tag.");
    return;
  }

  // 1. Determine API base — from data-key (base64) or data-api-base (dev fallback)
  let apiBase = scriptTag.getAttribute("data-api-base");
  if (!apiBase) {
    const encodedKey = scriptTag.getAttribute("data-key");
    if (encodedKey) {
      try { apiBase = atob(encodedKey); } catch (_) { }
    }
  }
  if (!apiBase) {
    console.error("LeadsAI: No API base configured.");
    return;
  }

  const API_URL = apiBase;

  // Base fetch helper — injects ngrok browser-warning bypass header on every request
  const apiFetch = (path, options = {}) => {
    return fetch(`${API_URL}${path}`, {
      ...options,
      headers: {
        'ngrok-skip-browser-warning': 'true',
        ...options.headers,
      },
    });
  };

  // 2. Fetch Widget Token (JWT)
  let widgetJwt = null;
  const companySlug = scriptTag.getAttribute("data-company");
  var _isDashboardPreview = false;

  if (companySlug) {
    try {
      const tokenRes = await apiFetch('/v1/widget/token', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ company_slug: companySlug })
      });
      if (tokenRes.ok) {
        const tokenData = await tokenRes.json();
        widgetJwt = tokenData.access_token;
      }
    } catch (e) {
      console.error("LeadsAI: Failed to fetch widget token", e);
    }
  } else {
    // Dashboard preview — use the logged-in user's JWT
    var dashToken = null;
    try { dashToken = localStorage.getItem('la_token'); } catch (_) { }
    if (dashToken) {
      widgetJwt = dashToken;
      _isDashboardPreview = true;
    }
  }

  const authHeaders = { 'Content-Type': 'application/json' };
  if (widgetJwt) authHeaders['Authorization'] = 'Bearer ' + widgetJwt;

  const authHeadersForm = {};
  if (widgetJwt) authHeadersForm['Authorization'] = 'Bearer ' + widgetJwt;

  // Auto-refresh token on 401 (secret rotation / expiry)
  async function _refreshToken() {
    if (!companySlug) return false;
    try {
      const tokenRes = await apiFetch('/v1/widget/token', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ company_slug: companySlug })
      });
      if (tokenRes.ok) {
        const tokenData = await tokenRes.json();
        widgetJwt = tokenData.access_token;
        authHeaders['Authorization'] = 'Bearer ' + widgetJwt;
        authHeadersForm['Authorization'] = 'Bearer ' + widgetJwt;
        return true;
      }
    } catch (e) {
      console.error("LeadsAI: Token refresh failed", e);
    }
    return false;
  }

  // Fetch with auto-retry on 401 (one retry after token refresh)
  async function _authedFetch(urlOrPath, options = {}) {
    // Support both full URLs and paths
    const fullUrl = urlOrPath.startsWith('http') ? urlOrPath : `${API_URL}${urlOrPath}`;
    options = {
      ...options,
      headers: {
        'ngrok-skip-browser-warning': 'true',
        ...options.headers,
      },
    };
    let resp = await fetch(fullUrl, options);
    if (resp.status === 401 && !_isDashboardPreview) {
      const refreshed = await _refreshToken();
      if (refreshed) {
        options.headers['Authorization'] = 'Bearer ' + widgetJwt;
        resp = await fetch(fullUrl, options);
      }
    }
    return resp;
  }

  // 3. Fetch Dynamic Configuration
  let config = {};
  try {
    const res = await apiFetch(
      companySlug
        ? `/v1/widget/config?slug=${encodeURIComponent(companySlug)}`
        : `/v1/widget/config`,
      { headers: authHeaders }
    );
    if (res.ok) {
      config = await res.json();
    }
  } catch (e) {
    console.error("LeadsAI: Failed to load config", e);
  }

  var cfg = {
    apiKey: '',
    apiBase: API_URL,
    position: config.position || 'bottom-right',
    color1: config.primary_color || '#2952e3',
    color2: config.accent_color || '#00d4f5',
    colorBotText: config.bot_text_color || '#ffffff',
    colorUserText: config.user_text_color || '#ffffff',
    bgColor: config.secondary_color || '#ffffff',
    bgImage: config.bg_image_url || '',
    greeting: config.greeting || "Hi there! 👋 I'm your AI assistant. How can I help you today?",
    name: config.business_name || "AI Assistant",
    logoUrl: config.logo_url || "",
    proactive: config.proactive_enabled !== false,
    tts: config.tts_enabled !== false,
    stt: config.stt_enabled !== false,
    cv: config.cv_search_enabled !== false,
    langs: config.languages || "en",
    piiAfter: config.pii_after_messages || 1,
    proactiveMsg: config.proactive_message || "Hi there! Need help? Chat with us!"
  };

  // ── Session / visitor IDs ───────────────────────────────────────
  var SESSION_ID = 'la_' + Math.random().toString(36).substr(2, 12);
  var VISITOR_ID = (function () {
    try {
      var v = localStorage.getItem('la_vid');
      if (!v) { v = 'v_' + Math.random().toString(36).substr(2, 16); localStorage.setItem('la_vid', v); }
      return v;
    } catch (e) { return 'v_' + Math.random().toString(36).substr(2, 16); }
  })();

  // ── State ───────────────────────────────────────────────────────
  var isOpen = false, isRecording = false, greetingShown = false;
  var msgCount = 0, piiCollected = false, _metaSent = false;
  var currentAudio = null, mediaRecorder = null, audioChunks = [];
  var pendingMessage = null; // Holds the first message until PII is collected
  var pageStart = Date.now();

  // ── Build shadow DOM container ──────────────────────────────────
  var host = document.createElement('div');
  host.id = 'leadsai-host';
  host.style.cssText = 'position:fixed;bottom:24px;' + (cfg.position.includes('right') ? 'right:24px' : 'left:24px') + ';z-index:2147483647;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",system-ui,sans-serif;';
  document.body.appendChild(host);
  var shadow = host.attachShadow({ mode: 'open' });

  // ── CSS ─────────────────────────────────────────────────────────
  var styleEl = document.createElement('style');
  styleEl.textContent = [
    ':host{all:initial}',
    '*{box-sizing:border-box;margin:0;padding:0}',

    /* Launcher */
    '.launcher{width:60px;height:60px;border-radius:50%;background:linear-gradient(135deg,' + cfg.color1 + ',' + cfg.color2 + ');border:none;cursor:pointer;box-shadow:0 4px 24px rgba(0,0,0,.3);display:flex;align-items:center;justify-content:center;transition:transform .2s,box-shadow .2s;outline:none;position:relative;overflow:hidden;}',
    '.launcher:hover{transform:scale(1.08);box-shadow:0 6px 32px rgba(0,0,0,.4)}',
    '.launcher svg{width:28px;height:28px;fill:white;transition:transform .3s;position:relative;z-index:2;}',
    '.launcher img.l-img{width:100%;height:100%;object-fit:contain;position:absolute;top:0;left:0;z-index:1;transition:opacity 0.3s;}',
    '.launcher.open svg{transform:rotate(90deg)}',
    '.launcher.open img.l-img{opacity:0;}',

    /* Proactive bubble */
    '.proactive{position:absolute;right:72px;bottom:8px;background:white;color:#1a1a2e;padding:10px 14px;border-radius:12px 12px 2px 12px;font-size:13px;font-weight:500;line-height:1.4;box-shadow:0 4px 20px rgba(0,0,0,.15);max-width:220px;min-width:160px;animation:slideIn .3s ease;cursor:pointer;display:none;}',
    '.proactive::after{content:"";position:absolute;right:-6px;bottom:12px;border:6px solid transparent;border-left-color:white}',
    '.pro-close{position:absolute;top:4px;right:6px;background:none;border:none;cursor:pointer;color:#94a3b8;font-size:14px;line-height:1;padding:2px 4px}',
    '@keyframes slideIn{from{opacity:0;transform:translateX(10px)}to{opacity:1;transform:translateX(0)}}',

    /* Chat window */
    '.chat-win{position:absolute;right:0;bottom:72px;width:380px;height:650px;background:#fff;border-radius:20px;box-shadow:0 20px 60px rgba(0,0,0,.25);display:flex;flex-direction:column;overflow:hidden;transform-origin:bottom right;transition:transform .3s cubic-bezier(.34,1.56,.64,1),opacity .2s;transform:scale(.8);opacity:0;pointer-events:none}',
    '.chat-win.open{transform:scale(1);opacity:1;pointer-events:all}',
    '@media(max-width:440px){.chat-win{width:calc(100vw - 24px);right:-4px}}',

    /* Header */
    '.chat-header{background:linear-gradient(135deg,' + cfg.color1 + ',' + cfg.color2 + ');padding:16px 16px 14px;color:white;flex-shrink:0}',
    '.hdr-top{display:flex;align-items:center;justify-content:space-between}',
    '.agent-info{display:flex;align-items:center;gap:10px}',
    '.agent-av{width:38px;height:38px;border-radius:50%;background:rgba(255,255,255,.2);display:flex;align-items:center;justify-content:center;font-size:18px;flex-shrink:0;border:2px solid rgba(255,255,255,.3);overflow:hidden;}',
    '.agent-av img{width:100%;height:100%;object-fit:contain;border-radius:50%;padding:2px;}',
    '.agent-name{font-weight:700;font-size:15px}',
    '.agent-status{font-size:12px;opacity:.85;display:flex;align-items:center;gap:4px}',
    '.sdot{width:7px;height:7px;border-radius:50%;background:#4ade80;display:inline-block;animation:blink 2s infinite}',
    '@keyframes blink{0%,100%{opacity:1}50%{opacity:.3}}',
    '.hdr-actions{display:flex;gap:6px}',
    '.hdr-btn{background:rgba(255,255,255,.15);border:none;width:32px;height:32px;border-radius:50%;color:white;cursor:pointer;font-size:14px;display:flex;align-items:center;justify-content:center;transition:background .15s;outline:none}',
    '.hdr-btn:hover{background:rgba(255,255,255,.25)}',
    '.lang-sel{border:1px solid rgba(255,255,255,.2);background:rgba(255,255,255,.1);font-size:12px;color:white;cursor:pointer;outline:none;padding:4px 6px;border-radius:6px;max-width:110px;-webkit-appearance:none;appearance:none;background-image:url("data:image/svg+xml,%3Csvg xmlns=\'http://www.w3.org/2000/svg\' width=\'10\' height=\'6\' viewBox=\'0 0 10 6\'%3E%3Cpath d=\'M0 0l5 6 5-6z\' fill=\'rgba(255,255,255,0.7)\'/%3E%3C/svg%3E");background-repeat:no-repeat;background-position:right 6px center;padding-right:18px}',
    '.lang-sel:hover{background:rgba(255,255,255,.2)}',
    '.lang-sel option{color:#1e293b;background:white;padding:4px 8px}',

    /* Messages */
    '.messages{flex:1;overflow-y:auto;padding:16px;display:flex;flex-direction:column;gap:12px;scroll-behavior:smooth;background:' + cfg.bgColor + '} ' + (cfg.bgImage ? `.messages{background-image:url('${cfg.bgImage}');background-size:cover;background-position:center}` : ""),
    '.messages::-webkit-scrollbar{width:4px}',
    '.messages::-webkit-scrollbar-thumb{background:#e2e8f0;border-radius:2px}',

    /* Message bubbles */
    '.msg{display:flex;gap:8px;max-width:100%}',
    '.msg.user{flex-direction:row-reverse}',
    '.msg-av{width:28px;height:28px;border-radius:50%;background:' + cfg.color1 + ';color:white;font-size:12px;display:flex;align-items:center;justify-content:center;flex-shrink:0;margin-top:2px;font-weight:600;overflow:hidden;}',
    '.msg-av img{width:100%;height:100%;object-fit:contain;padding:4px;}',
    '.msg.user .msg-av{background:' + cfg.color2 + '}',
    '.bubble{max-width:calc(100% - 44px);padding:10px 14px;border-radius:16px;font-size:12.5px;line-height:1.55;word-break:break-word}',
    '.msg.assistant .bubble{background:' + cfg.color1 + ';color:' + cfg.colorBotText + ';border-radius:4px 16px 16px 16px}',
    '.msg.user .bubble{background:' + cfg.color2 + ';color:' + cfg.colorUserText + ';border-radius:16px 4px 16px 16px}',

    /* Markdown inside bubbles */
    '.bubble h1,.bubble h2,.bubble h3{font-weight:700;margin:6px 0 2px}',
    '.bubble h1{font-size:1.1em}',
    '.bubble h2{font-size:1.05em}',
    '.bubble h3{font-size:1em}',
    '.bubble p{margin:4px 0}',
    '.bubble ul,.bubble ol{margin:4px 0 4px 16px;padding:0}',
    '.bubble li{margin:2px 0}',
    '.bubble code{background:rgba(0,0,0,.15);padding:1px 4px;border-radius:3px;font-size:0.9em;font-family:monospace}',
    '.bubble pre{background:rgba(0,0,0,.15);padding:8px 10px;border-radius:6px;overflow-x:auto;margin:6px 0;font-size:0.85em}',
    '.bubble pre code{background:none;padding:0}',
    '.bubble strong,.bubble b{font-weight:700}',
    '.bubble em,.bubble i{font-style:italic}',
    '.bubble a{color:inherit;text-decoration:underline;opacity:0.9}',
    '.bubble a:hover{opacity:1}',
    '.bubble hr{border:none;border-top:1px solid rgba(255,255,255,.2);margin:6px 0}',
    '.bubble blockquote{border-left:3px solid rgba(255,255,255,.3);padding-left:8px;margin:4px 0;opacity:0.9}',

    /* Typing */
    '.typing-bbl{padding:12px 16px}',
    '.typing-dots{display:flex;gap:4px;align-items:center}',
    '.typing-dots span{width:7px;height:7px;border-radius:50%;background:rgba(255,255,255,0.7);display:inline-block;animation:bounce 1.4s ease-in-out infinite}',
    '.typing-dots span:nth-child(2){animation-delay:.2s}',
    '.typing-dots span:nth-child(3){animation-delay:.4s}',
    '@keyframes bounce{0%,60%,100%{transform:translateY(0)}30%{transform:translateY(-6px)}}',

    /* Audio btn — hidden, TTS auto-plays */
    '.audio-btn{display:none}',

    /* Source chips — only shown for products with a link */
    '.src-chips{display:flex;flex-wrap:wrap;gap:4px;margin-top:6px}',
    '.src-chip{background:rgba(255,255,255,.15);color:' + cfg.colorBotText + ';padding:2px 8px;border-radius:20px;font-size:11px;border:1px solid rgba(255,255,255,.3);text-decoration:none;cursor:pointer;display:inline-flex;align-items:center;gap:3px}',
    '.src-chip:hover{background:rgba(255,255,255,.28)}',

    /* Product cards */
    '.product-strip{display:flex;gap:8px;overflow-x:auto;padding:8px 0 4px;margin-top:6px;scroll-snap-type:x mandatory;}',
    '.product-strip::-webkit-scrollbar{height:3px}',
    '.product-strip::-webkit-scrollbar-thumb{background:rgba(255,255,255,0.3);border-radius:2px}',
    '.product-card{flex-shrink:0;width:140px;background:rgba(255,255,255,0.12);border:1px solid rgba(255,255,255,0.2);border-radius:12px;padding:10px;scroll-snap-align:start;transition:background .15s}',
    '.product-card:hover{background:rgba(255,255,255,0.2)}',
    '.product-card .p-icon{width:100%;height:60px;background:rgba(255,255,255,0.1);border-radius:8px;display:flex;align-items:center;justify-content:center;font-size:24px;margin-bottom:6px;overflow:hidden;}',
    '.product-card .p-icon img{width:100%;height:100%;object-fit:cover;}',
    '.product-card .p-name{font-size:12px;font-weight:600;color:' + cfg.colorBotText + ';line-height:1.3;margin-bottom:3px;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden;}',
    '.product-card .p-price{font-size:11px;color:rgba(255,255,255,0.8);font-weight:500}',

    /* PII card */
    '.pii-card{background:linear-gradient(135deg,#f0f4ff,#e8f4f8);border:1px solid #c7d2fe;border-radius:12px;padding:16px;margin:4px 0}',
    '.pii-card h4{font-size:14px;color:#1e293b;margin-bottom:4px}',
    '.pii-card p{font-size:12px;color:#64748b;margin-bottom:12px}',
    '.pii-form{display:flex;flex-direction:column;gap:8px}',
    '.pii-inp{border:1px solid #e2e8f0;border-radius:8px;padding:8px 12px;font-size:13px;outline:none;transition:border-color .15s;width:100%;font-family:inherit;color:#1e293b;background:white}',
    '.pii-inp:focus{border-color:' + cfg.color1 + '}',
    '.pii-btn{background:linear-gradient(135deg,' + cfg.color1 + ',' + cfg.color2 + ');color:white;border:none;border-radius:8px;padding:8px 16px;font-size:13px;cursor:pointer;font-weight:600;font-family:inherit}',
    '.pii-btn:disabled{opacity:0.5;cursor:not-allowed}',

    /* Input area */
    '.input-area{padding:10px 12px 12px;background:white;border-top:1px solid #f1f5f9;flex-shrink:0}',
    '.input-row{display:flex;gap:6px;align-items:flex-end}',
    '.input-box{flex:1;min-height:40px;max-height:100px;border:1.5px solid #e2e8f0;border-radius:20px;padding:9px 14px;font-size:14px;outline:none;resize:none;overflow-y:auto;font-family:inherit;line-height:1.4;transition:border-color .15s;color:#1e293b;background:#f8fafc}',
    '.input-box:focus{border-color:' + cfg.color1 + ';background:white}',
    '.input-box::placeholder{color:#94a3b8}',
    '.act-btn{width:36px;height:36px;border-radius:50%;border:none;cursor:pointer;display:flex;align-items:center;justify-content:center;font-size:15px;transition:all .15s;outline:none;background:#f1f5f9;color:#64748b;flex-shrink:0}',
    '.act-btn:hover{background:#e2e8f0;color:#475569}',
    '.send-btn{background:linear-gradient(135deg,' + cfg.color1 + ',' + cfg.color2 + ');color:white}',
    '.send-btn:hover{opacity:.9;transform:scale(1.05)}',
    '.send-btn:disabled{opacity:.5;cursor:not-allowed;transform:none}',
    '.mic-btn.recording{background:#ef4444;color:white;animation:micPulse 1.2s ease-in-out infinite}',
    '@keyframes micPulse{0%,100%{box-shadow:0 0 0 0 rgba(239,68,68,0.4)}50%{box-shadow:0 0 0 8px rgba(239,68,68,0)}}',
    '.powered{text-align:center;font-size:11px;color:#94a3b8;padding-top:6px}',
    '.powered a{color:' + cfg.color2 + ';text-decoration:none}',

    /* Plus menu */
    '.plus-menu{position:absolute;bottom:100%;left:0;margin-bottom:8px;background:white;border:1px solid #e2e8f0;border-radius:12px;box-shadow:0 8px 30px rgba(0,0,0,.15);overflow:hidden;min-width:180px;transform-origin:bottom left;transition:transform .15s,opacity .15s;transform:scale(.9);opacity:0;pointer-events:none;z-index:10}',
    '.plus-menu.open{transform:scale(1);opacity:1;pointer-events:all}',
    '.plus-item{display:flex;align-items:center;gap:10px;padding:10px 14px;cursor:pointer;font-size:13px;color:#1e293b;transition:background .1s;border:none;background:none;width:100%;text-align:left;font-family:inherit}',
    '.plus-item:hover{background:#f1f5f9}',
    '.plus-item .p-ico{font-size:18px}',

    /* Image preview in bubble */
    '.img-preview{max-width:200px;border-radius:10px;display:block;margin-bottom:6px}',
    '.img-caption{font-size:12px;color:rgba(255,255,255,.8)}',
  ].join('');
  shadow.appendChild(styleEl);
  //To add Logo in the button:
  //${cfg.logoUrl ? `<img src="${cfg.logoUrl}" class="l-img" alt="logo" onerror="this.style.display='none'"/>` : ""} 
  // ── HTML Structure ──────────────────────────────────────────────
  var root = document.createElement('div');
  root.innerHTML = `
    <div class="proactive" id="pro">
      <button class="pro-close" id="proClose">×</button>
      <span id="proText">${cfg.proactiveMsg}</span>
    </div>

    <button class="launcher" id="launcher" aria-label="Open chat">

      <svg id="launchIco" viewBox="0 0 24 24"><path id="launchPath" d="M20 2H4c-1.1 0-2 .9-2 2v18l4-4h14c1.1 0 2-.9 2-2V4c0-1.1-.9-2-2-2zm-2 12H6v-2h12v2zm0-3H6V9h12v2zm0-3H6V6h12v2z"/></svg>
    </button>

    <div class="chat-win" id="chatWin">
      <div class="chat-header">
        <div class="hdr-top">
          <div class="agent-info">
            <div class="agent-av">
              ${cfg.logoUrl ? `<img src="${cfg.logoUrl}" style="object-fit:contain;" onerror="this.style.display='none'"/>` : "🤖"}
            </div>
            <div>
              <div class="agent-name">${cfg.name}</div>
              <div class="agent-status"><span class="sdot"></span> Online</div>
            </div>
          </div>
          <div class="hdr-actions">
            <select class="lang-sel" id="langSel">
              <option value="auto">🌐 Auto</option>
              ${(function () {
      var _langMap = {
        en: 'English', hi: 'हिन्दी', ta: 'தமிழ்', te: 'తెలుగు', bn: 'বাংলা',
        mr: 'मराठी', gu: 'ગુજરાતી', kn: 'ಕನ್ನಡ', ml: 'മലയാളം', pa: 'ਪੰਜਾਬੀ',
        es: 'Español', fr: 'Français', de: 'Deutsch', pt: 'Português',
        ar: 'العربية', zh: '中文', ja: '日本語', ko: '한국어', ru: 'Русский', it: 'Italiano'
      };
      return cfg.langs.split(',').map(function (l) {
        var c = l.trim(); var name = _langMap[c] || c.toUpperCase();
        return '<option value="' + c + '">' + name + '</option>';
      }).join('');
    })()}
            </select>
            <button class="hdr-btn" id="closeBtn" title="Close">✕</button>
          </div>
        </div>
      </div>

      <div class="messages" id="msgs"></div>

      <div class="input-area" id="inputArea">
        <div class="input-row" style="position:relative;">
          <button class="act-btn plus-btn" id="plusBtn" title="Attach" style="display: ${cfg.cv ? 'flex' : 'none'};">
            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>
          </button>
          <div class="plus-menu" id="plusMenu">
            <button class="plus-item" id="pmCamera"><span class="p-ico">📷</span> Take Photo</button>
            <button class="plus-item" id="pmGallery"><span class="p-ico">🖼️</span> Upload Image</button>
          </div>
          <textarea class="input-box" id="inputBox" placeholder="Ask me anything..." rows="1"></textarea>
          <button class="act-btn mic-btn" id="micBtn" title="Voice input" style="display: ${cfg.stt ? 'flex' : 'none'};">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M12 15c1.66 0 2.99-1.34 2.99-3L15 6c0-1.66-1.34-3-3-3S9 4.34 9 6v6c0 1.66 1.34 3 3 3zm5.3-3c0 3-2.54 5.1-5.3 5.1S6.7 15 6.7 12H5c0 3.41 2.72 6.23 6 6.72V21h2v-2.28c3.28-.49 6-3.31 6-6.72h-1.7z"/></svg>
          </button>
          <button class="act-btn send-btn" id="sendBtn">➤</button>
        </div>
        <div class="powered">Powered by <a href="https://leadsai.winssoft.com" target="_blank">Wins Soft - Leads AI</a></div>
      </div>
      <input type="file" id="cvInputCamera" accept="image/*" capture="environment" style="display:none">
      <input type="file" id="cvInputGallery" accept="image/*" style="display:none">
    </div>
  `;
  shadow.appendChild(root);

  // ── Element shortcuts ───────────────────────────────────────────
  function g(id) { return shadow.getElementById(id); }
  var launcher = g('launcher');
  var chatWin = g('chatWin');
  var msgs = g('msgs');
  var inputBox = g('inputBox');
  var sendBtn = g('sendBtn');
  var closeBtn = g('closeBtn');
  var plusBtn = g('plusBtn');
  var plusMenu = g('plusMenu');
  var micBtn = g('micBtn');
  var proEl = g('pro');
  var langSel = g('langSel');
  var cvInputCamera = g('cvInputCamera');
  var cvInputGallery = g('cvInputGallery');

  // ── Open / Close ────────────────────────────────────────────────
  function openChat() {
    isOpen = true;
    chatWin.classList.add('open');
    launcher.classList.add('open');
    g('launchPath').setAttribute('d', 'M19 6.41L17.59 5 12 10.59 6.41 5 5 6.41 10.59 12 5 17.59 6.41 19 12 13.41 17.59 19 19 17.59 13.41 12z');
    proEl.style.display = 'none';
    // Bug 1 fix: only show greeting once across all open/close cycles
    if (!greetingShown) { addMsg('assistant', cfg.greeting); greetingShown = true; }
    setTimeout(function () { inputBox.focus(); }, 300);
  }

  function closeChat() {
    isOpen = false;
    chatWin.classList.remove('open');
    launcher.classList.remove('open');
    g('launchPath').setAttribute('d', 'M20 2H4c-1.1 0-2 .9-2 2v18l4-4h14c1.1 0 2-.9 2-2V4c0-1.1-.9-2-2-2zm-2 12H6v-2h12v2zm0-3H6V9h12v2zm0-3H6V6h12v2z');
  }

  launcher.addEventListener('click', function () { isOpen ? closeChat() : openChat(); });
  closeBtn.addEventListener('click', closeChat);
  g('proClose').addEventListener('click', function () { proEl.style.display = 'none'; });
  proEl.addEventListener('click', function (e) {
    if (e.target.id !== 'proClose') { openChat(); proEl.style.display = 'none'; }
  });

  if (cfg.proactive) {
    setTimeout(() => { if (!isOpen && msgCount === 0) proEl.style.display = 'block'; }, 4000);
  }

  // ── Plus Menu (attach image) ────────────────────────────────────
  var plusOpen = false;
  plusBtn.addEventListener('click', function (e) {
    e.stopPropagation();
    plusOpen = !plusOpen;
    plusMenu.classList.toggle('open', plusOpen);
  });
  // Close menu on outside click
  shadow.addEventListener('click', function (e) {
    if (plusOpen && !plusMenu.contains(e.target) && e.target !== plusBtn) {
      plusOpen = false;
      plusMenu.classList.remove('open');
    }
  });

  g('pmCamera').addEventListener('click', function () {
    plusOpen = false; plusMenu.classList.remove('open');
    cvInputCamera.click();
  });
  g('pmGallery').addEventListener('click', function () {
    plusOpen = false; plusMenu.classList.remove('open');
    cvInputGallery.click();
  });

  cvInputCamera.addEventListener('change', function (e) { if (e.target.files && e.target.files[0]) handleImage(e.target.files[0]); cvInputCamera.value = ''; });
  cvInputGallery.addEventListener('change', function (e) { if (e.target.files && e.target.files[0]) handleImage(e.target.files[0]); cvInputGallery.value = ''; });

  // ── Lightweight Markdown → HTML parser ──────────────────────────
  function _md(src) {
    if (!src) return '';
    // Escape HTML to prevent XSS
    var s = src.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

    // Code blocks (```...```)
    s = s.replace(/```(\w*)\n?([\s\S]*?)```/g, function (_, lang, code) {
      return '<pre><code>' + code.trim() + '</code></pre>';
    });

    // Split into lines for block-level parsing
    var lines = s.split('\n');
    var html = [];
    var inList = false, listType = '';

    for (var i = 0; i < lines.length; i++) {
      var line = lines[i];

      // Headers
      if (/^### (.+)/.test(line)) { closeLst(); html.push('<h3>' + inline(line.slice(4)) + '</h3>'); continue; }
      if (/^## (.+)/.test(line)) { closeLst(); html.push('<h2>' + inline(line.slice(3)) + '</h2>'); continue; }
      if (/^# (.+)/.test(line)) { closeLst(); html.push('<h1>' + inline(line.slice(2)) + '</h1>'); continue; }

      // Horizontal rule
      if (/^[-*_]{3,}\s*$/.test(line)) { closeLst(); html.push('<hr>'); continue; }

      // Blockquote
      if (/^>\s?(.*)/.test(line)) { closeLst(); html.push('<blockquote>' + inline(line.replace(/^>\s?/, '')) + '</blockquote>'); continue; }

      // Unordered list
      if (/^[\-\*]\s+(.+)/.test(line)) {
        if (!inList || listType !== 'ul') { closeLst(); html.push('<ul>'); inList = true; listType = 'ul'; }
        html.push('<li>' + inline(line.replace(/^[\-\*]\s+/, '')) + '</li>');
        continue;
      }

      // Ordered list
      if (/^\d+[\.\)]\s+(.+)/.test(line)) {
        if (!inList || listType !== 'ol') { closeLst(); html.push('<ol>'); inList = true; listType = 'ol'; }
        html.push('<li>' + inline(line.replace(/^\d+[\.\)]\s+/, '')) + '</li>');
        continue;
      }

      // Empty line
      if (line.trim() === '') { closeLst(); continue; }

      // Paragraph
      closeLst();
      html.push('<p>' + inline(line) + '</p>');
    }
    closeLst();
    return html.join('');

    function closeLst() {
      if (inList) { html.push(listType === 'ul' ? '</ul>' : '</ol>'); inList = false; listType = ''; }
    }
    function inline(t) {
      return t
        .replace(/`([^`]+)`/g, '<code>$1</code>')           // inline code
        .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')    // bold
        .replace(/__(.+?)__/g, '<strong>$1</strong>')         // bold alt
        .replace(/\*(.+?)\*/g, '<em>$1</em>')                // italic
        .replace(/_(.+?)_/g, '<em>$1</em>')                  // italic alt
        .replace(/\[([^\]]+)\]\(([^)]+)\)/g, '<a href="$2" target="_blank" rel="noopener">$1</a>'); // links
    }
  }

  // ── Add message ─────────────────────────────────────────────────
  function addMsg(role, text, audioUrl, sources, products) {
    var el = document.createElement('div');
    el.className = 'msg ' + role;

    var av = document.createElement('div');
    av.className = 'msg-av';
    av.innerHTML = role === 'assistant' ? (cfg.logoUrl ? `<img src="${cfg.logoUrl}" onerror="this.style.display='none'"/>` : '🤖') : '👤';

    var bbl = document.createElement('div');
    bbl.className = 'bubble';
    if (role === 'assistant') {
      bbl.innerHTML = _md(text);
    } else {
      bbl.textContent = text;
    }

    if (audioUrl && role === 'assistant' && cfg.tts) {
      var aBtn = document.createElement('button');
      aBtn.className = 'audio-btn';
      aBtn.textContent = '🔊 Play';
      (function (url, btn) {
        btn.addEventListener('click', function () { playAudio(url, btn); });
      })(audioUrl, aBtn);
      bbl.appendChild(aBtn);
    }

    // Bug 2 fix: only render product sources that have a source_url as clickable links
    /* QUICKLINKS DISABLED
    if (sources && sources.length) {
      // ...
    }
    */

    // Product cards strip
    /* PRODUCT STRIP DISABLED
    if (products && products.length > 0 && role === 'assistant') {
       // ...
    }
    */

    el.appendChild(av);
    el.appendChild(bbl);
    msgs.appendChild(el);
    msgs.scrollTop = msgs.scrollHeight;
    return el;
  }

  function showTyping() {
    var el = document.createElement('div');
    el.className = 'msg assistant';
    el.id = 'typing-ind';
    el.innerHTML = '<div class="msg-av">' + (cfg.logoUrl ? `<img src="${cfg.logoUrl}" onerror="this.style.display='none'"/>` : '🤖') + '</div><div class="bubble typing-bbl" style="background:' + cfg.color1 + ';"><div class="typing-dots"><span></span><span></span><span></span></div></div>';
    msgs.appendChild(el);
    msgs.scrollTop = msgs.scrollHeight;
  }

  function hideTyping() {
    var el = shadow.getElementById('typing-ind');
    if (el) el.remove();
  }

  function playAudio(url, btn) {
    if (currentAudio) { try { currentAudio.pause(); } catch (e) { } currentAudio = null; }
    if (url && url.indexOf('data:') === 0) {
      var a = new Audio(url);
      currentAudio = a;
      btn.textContent = '⏸ Playing';
      a.play().catch(function () { });
      a.onended = function () { btn.textContent = '🔊 Play'; currentAudio = null; };
    }
  }

  // ── PII Prompt (shown before first response) ────────────────────
  function showPIIPrompt() {
    var card = document.createElement('div');
    card.className = 'pii-card';
    card.id = 'pii-card';

    var h4 = document.createElement('h4');
    h4.textContent = '👋 Before we begin...';

    var p = document.createElement('p');
    p.textContent = 'Please share your details so we can assist you better and follow up if needed.';

    var form = document.createElement('div');
    form.className = 'pii-form';

    var nameInp = document.createElement('input');
    nameInp.className = 'pii-inp'; nameInp.placeholder = 'Your name *'; nameInp.type = 'text';

    var phoneRow = document.createElement('div');
    phoneRow.style.display = 'flex';
    phoneRow.style.gap = '8px';

    var codeList = document.createElement('datalist');
    codeList.id = 'country-codes';
    var codes = [
      '+93 (Afghanistan)', '+355 (Albania)', '+213 (Algeria)', '+376 (Andorra)', '+244 (Angola)',
      '+54 (Argentina)', '+374 (Armenia)', '+61 (Australia)', '+43 (Austria)', '+994 (Azerbaijan)',
      '+973 (Bahrain)', '+880 (Bangladesh)', '+375 (Belarus)', '+32 (Belgium)', '+501 (Belize)',
      '+229 (Benin)', '+975 (Bhutan)', '+591 (Bolivia)', '+387 (Bosnia)', '+267 (Botswana)',
      '+55 (Brazil)', '+673 (Brunei)', '+359 (Bulgaria)', '+226 (Burkina Faso)', '+257 (Burundi)',
      '+855 (Cambodia)', '+237 (Cameroon)', '+1 (Canada)', '+236 (Central African Republic)', '+235 (Chad)',
      '+56 (Chile)', '+86 (China)', '+57 (Colombia)', '+269 (Comoros)', '+243 (Congo DR)',
      '+506 (Costa Rica)', '+385 (Croatia)', '+53 (Cuba)', '+357 (Cyprus)', '+420 (Czech Republic)',
      '+45 (Denmark)', '+253 (Djibouti)', '+593 (Ecuador)', '+20 (Egypt)', '+503 (El Salvador)',
      '+291 (Eritrea)', '+372 (Estonia)', '+251 (Ethiopia)', '+679 (Fiji)', '+358 (Finland)',
      '+33 (France)', '+241 (Gabon)', '+220 (Gambia)', '+995 (Georgia)', '+49 (Germany)',
      '+233 (Ghana)', '+30 (Greece)', '+502 (Guatemala)', '+224 (Guinea)', '+592 (Guyana)',
      '+509 (Haiti)', '+504 (Honduras)', '+852 (Hong Kong)', '+36 (Hungary)', '+354 (Iceland)',
      '+91 (India)', '+62 (Indonesia)', '+98 (Iran)', '+964 (Iraq)', '+353 (Ireland)',
      '+972 (Israel)', '+39 (Italy)', '+225 (Ivory Coast)', '+1876 (Jamaica)', '+81 (Japan)',
      '+962 (Jordan)', '+7 (Kazakhstan)', '+254 (Kenya)', '+965 (Kuwait)', '+996 (Kyrgyzstan)',
      '+856 (Laos)', '+371 (Latvia)', '+961 (Lebanon)', '+231 (Liberia)', '+218 (Libya)',
      '+423 (Liechtenstein)', '+370 (Lithuania)', '+352 (Luxembourg)', '+853 (Macau)',
      '+261 (Madagascar)', '+265 (Malawi)', '+60 (Malaysia)', '+960 (Maldives)', '+223 (Mali)',
      '+356 (Malta)', '+222 (Mauritania)', '+230 (Mauritius)', '+52 (Mexico)', '+373 (Moldova)',
      '+377 (Monaco)', '+976 (Mongolia)', '+382 (Montenegro)', '+212 (Morocco)', '+258 (Mozambique)',
      '+95 (Myanmar)', '+264 (Namibia)', '+977 (Nepal)', '+31 (Netherlands)', '+64 (New Zealand)',
      '+505 (Nicaragua)', '+227 (Niger)', '+234 (Nigeria)', '+389 (North Macedonia)', '+47 (Norway)',
      '+968 (Oman)', '+92 (Pakistan)', '+507 (Panama)', '+595 (Paraguay)', '+51 (Peru)',
      '+63 (Philippines)', '+48 (Poland)', '+351 (Portugal)', '+974 (Qatar)', '+40 (Romania)',
      '+7 (Russia)', '+250 (Rwanda)', '+966 (Saudi Arabia)', '+221 (Senegal)', '+381 (Serbia)',
      '+65 (Singapore)', '+421 (Slovakia)', '+386 (Slovenia)', '+252 (Somalia)', '+27 (South Africa)',
      '+82 (South Korea)', '+211 (South Sudan)', '+34 (Spain)', '+94 (Sri Lanka)', '+249 (Sudan)',
      '+46 (Sweden)', '+41 (Switzerland)', '+963 (Syria)', '+886 (Taiwan)', '+992 (Tajikistan)',
      '+255 (Tanzania)', '+66 (Thailand)', '+228 (Togo)', '+216 (Tunisia)', '+90 (Turkey)',
      '+993 (Turkmenistan)', '+256 (Uganda)', '+380 (Ukraine)', '+971 (UAE)', '+44 (UK)',
      '+1 (USA)', '+598 (Uruguay)', '+998 (Uzbekistan)', '+58 (Venezuela)', '+84 (Vietnam)',
      '+967 (Yemen)', '+260 (Zambia)', '+263 (Zimbabwe)'
    ];
    codeList.innerHTML = codes.map(function (c) { return '<option value="' + c.split(' ')[0] + '">' + c + '</option>'; }).join('');

    var codeInp = document.createElement('input');
    codeInp.className = 'pii-inp'; codeInp.placeholder = 'Code'; codeInp.setAttribute('list', 'country-codes');
    codeInp.style.width = '90px'; codeInp.value = '+91';

    var phoneInp = document.createElement('input');
    phoneInp.className = 'pii-inp'; phoneInp.placeholder = 'Mobile number *'; phoneInp.type = 'tel'; phoneInp.style.flex = '1';

    phoneRow.appendChild(codeList);
    phoneRow.appendChild(codeInp);
    phoneRow.appendChild(phoneInp);

    var emailInp = document.createElement('input');
    emailInp.className = 'pii-inp'; emailInp.placeholder = 'Email (optional)'; emailInp.type = 'email';

    var submitBtn = document.createElement('button');
    submitBtn.className = 'pii-btn'; submitBtn.textContent = 'Start Chat →';

    var errDiv = document.createElement('div');
    errDiv.style.cssText = 'font-size:12px;color:#ef4444;display:none;';

    // Order: Name → Mobile → Email
    form.appendChild(nameInp);
    form.appendChild(phoneRow);
    form.appendChild(emailInp);
    form.appendChild(errDiv);
    form.appendChild(submitBtn);
    card.appendChild(h4);
    card.appendChild(p);
    card.appendChild(form);
    msgs.appendChild(card);
    msgs.scrollTop = msgs.scrollHeight;
    nameInp.focus();

    submitBtn.addEventListener('click', function () {
      var name = nameInp.value.trim();
      var email = emailInp.value.trim();
      var phone = (codeInp.value.trim() + ' ' + phoneInp.value.trim()).trim();

      // Reset border colors
      nameInp.style.borderColor = '#e2e8f0';
      phoneInp.style.borderColor = '#e2e8f0';
      emailInp.style.borderColor = '#e2e8f0';
      errDiv.style.display = 'none';

      // Validate name & phone
      if (!name || !phoneInp.value.trim()) {
        if (!name) nameInp.style.borderColor = '#ef4444';
        if (!phoneInp.value.trim()) phoneInp.style.borderColor = '#ef4444';
        errDiv.textContent = 'Name and mobile number are required.';
        errDiv.style.display = 'block';
        return;
      }
      // Validate country code format
      if (!/^\+\d{1,4}$/.test(codeInp.value.trim())) {
        codeInp.style.borderColor = '#ef4444';
        errDiv.textContent = 'Please select a valid country code.';
        errDiv.style.display = 'block';
        return;
      }
      // Validate phone digits (5-15 digits)
      if (!/^\d{5,15}$/.test(phoneInp.value.trim().replace(/[\s\-]/g, ''))) {
        phoneInp.style.borderColor = '#ef4444';
        errDiv.textContent = 'Please enter a valid mobile number (digits only).';
        errDiv.style.display = 'block';
        return;
      }
      // Validate email structure if provided (user@domain.tld — TLD must be 2+ chars)
      if (email && !/^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$/.test(email)) {
        emailInp.style.borderColor = '#ef4444';
        errDiv.textContent = 'Please enter a valid email address (e.g. user@example.com).';
        errDiv.style.display = 'block';
        return;
      }

      piiCollected = true;
      submitBtn.disabled = true;
      submitBtn.textContent = 'Starting...';

      // Save PII via API
      _authedFetch(API_URL + '/v1/lead', {
        method: 'POST',
        headers: authHeaders,
        body: JSON.stringify({ session_id: SESSION_ID, name: name, email: email, phone: phone, consent: true })
      }).catch(function () { });

      // Remove PII card
      card.remove();

      // Now send the pending message
      if (pendingMessage) {
        var held = pendingMessage;
        pendingMessage = null;
        _doSendMessage(held);
      }
    });

    // Also handle Enter key in inputs
    [nameInp, emailInp, phoneInp].forEach(function (inp) {
      inp.addEventListener('keydown', function (e) {
        if (e.key === 'Enter') { e.preventDefault(); submitBtn.click(); }
      });
    });
  }

  // ── Send message ─────────────────────────────────────────────────
  inputBox.addEventListener('keydown', function (e) {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      sendMessage(inputBox.value);
    }
  });
  inputBox.addEventListener('input', function () {
    inputBox.style.height = 'auto';
    inputBox.style.height = Math.min(inputBox.scrollHeight, 100) + 'px';
  });
  sendBtn.addEventListener('click', function () { sendMessage(inputBox.value); });

  function sendMessage(text) {
    if (!text || !text.trim()) return;
    text = text.trim();

    addMsg('user', text);
    inputBox.value = '';
    inputBox.style.height = 'auto';
    msgCount++;

    // If PII not collected yet and limit exceeded locally, hold message
    if (!piiCollected && msgCount > cfg.piiAfter) {
      pendingMessage = text;
      showPIIPrompt();
      return;
    }

    _doSendMessage(text);
  }

  async function _doSendMessage(text) {
    showTyping();

    try {
      var chatBody = {
        session_id: SESSION_ID,
        message: text,
        language: langSel ? langSel.value : 'auto',
        tts_enabled: cfg.tts
      };
      // Send client metadata on first message only
      if (!_metaSent) {
        _metaSent = true;
        try {
          chatBody.screen_resolution = screen.width + 'x' + screen.height;
          chatBody.page_url = window.location.href;
          chatBody.client_timezone = Intl.DateTimeFormat().resolvedOptions().timeZone;
        } catch (_e) { }
      }
      const resp = await _authedFetch(API_URL + '/v1/chat', {
        method: 'POST',
        headers: authHeaders,
        body: JSON.stringify(chatBody)
      });
      const data = await resp.json();
      hideTyping();
      if (!resp.ok) {
        addMsg('assistant', data.detail || "Server error.");
      } else {
        if (data.session_id) SESSION_ID = data.session_id;
        addMsg('assistant', data.message, data.audio_url, data.sources, data.products || data.suggested_products);
        // Bug 3 fix: auto-play TTS audio if available, no play button needed
        if (data.audio_url && cfg.tts) {
          playAudio(data.audio_url, { textContent: '' });
        }
        if (data.needs_pii_prompt && !piiCollected) {
          showPIIPrompt();
        }
      }
    } catch (e) {
      hideTyping();
      addMsg('assistant', "Network error. Make sure the Service is Online.");
    }
  }

  // ── Microphone (Voice-to-Text) ──────────────────────────────────
  micBtn.addEventListener('click', function () {
    if (!isRecording) startMicRecording();
    else stopMicRecording();
  });

  function startMicRecording() {
    if (!navigator.mediaDevices || !navigator.mediaDevices.getUserMedia) {
      addMsg('assistant', 'Microphone is not supported in this browser.');
      return;
    }
    navigator.mediaDevices.getUserMedia({ audio: true })
      .then(function (stream) {
        var mime = MediaRecorder.isTypeSupported('audio/webm') ? 'audio/webm' : 'audio/ogg';
        mediaRecorder = new MediaRecorder(stream, { mimeType: mime });
        audioChunks = [];
        mediaRecorder.ondataavailable = function (e) {
          if (e.data.size > 0) audioChunks.push(e.data);
        };
        mediaRecorder.onstop = function () {
          stream.getTracks().forEach(function (t) { t.stop(); });
          transcribeAudio();
        };
        mediaRecorder.start(250);
        isRecording = true;
        micBtn.classList.add('recording');
        micBtn.title = 'Stop recording';
      })
      .catch(function () {
        addMsg('assistant', 'Microphone access denied. Please allow microphone permission.');
      });
  }

  function stopMicRecording() {
    if (mediaRecorder && mediaRecorder.state !== 'inactive') {
      mediaRecorder.stop();
    }
    isRecording = false;
    micBtn.classList.remove('recording');
    micBtn.title = 'Voice input';
  }

  async function transcribeAudio() {
    if (audioChunks.length === 0) return;
    var blob = new Blob(audioChunks, { type: mediaRecorder.mimeType });
    audioChunks = [];

    // Convert to base64 and send to STT
    var reader = new FileReader();
    reader.onloadend = async function () {
      var base64 = reader.result.split(',')[1];
      try {
        var resp = await _authedFetch(API_URL + '/v1/stt', {
          method: 'POST',
          headers: authHeaders,
          body: JSON.stringify({ audio_b64: base64, session_id: SESSION_ID, language: langSel ? langSel.value : 'auto' })
        });
        var data = await resp.json();
        if (data.text && data.text.trim()) {
          inputBox.value = data.text.trim();
          inputBox.style.height = 'auto';
          inputBox.style.height = Math.min(inputBox.scrollHeight, 100) + 'px';
          inputBox.focus();
        }
      } catch (e) {
        console.error('STT error:', e);
      }
    };
    reader.readAsDataURL(blob);
  }

  // ── Image Upload (CV Search) ───────────────────────────────────
  function addImageMsg(dataUrl) {
    var el = document.createElement('div');
    el.className = 'msg user';
    var av = document.createElement('div');
    av.className = 'msg-av';
    av.textContent = '👤';
    var bbl = document.createElement('div');
    bbl.className = 'bubble';
    bbl.style.padding = '6px';
    bbl.style.background = cfg.color2;
    var img = document.createElement('img');
    img.className = 'img-preview';
    img.src = dataUrl;
    var cap = document.createElement('div');
    cap.className = 'img-caption';
    cap.textContent = '🔎 Searching...';
    bbl.appendChild(img);
    bbl.appendChild(cap);
    el.appendChild(av);
    el.appendChild(bbl);
    msgs.appendChild(el);
    msgs.scrollTop = msgs.scrollHeight;
  }

  async function handleImage(file) {
    // If PII not collected and limit met, prompt first
    if (!piiCollected && msgCount >= cfg.piiAfter) {
      pendingMessage = '__IMAGE_SEARCH__';
      showPIIPrompt();
      // Store image for after PII
      window._pendingImageFile = file;
      return;
    }

    var reader = new FileReader();
    reader.onload = function (e) {
      addImageMsg(e.target.result);
    };
    reader.readAsDataURL(file);
    showTyping();

    var formData = new FormData();
    formData.append('file', file);
    if (SESSION_ID) formData.append('session_id', SESSION_ID);

    try {
      const resp = await _authedFetch(API_URL + '/v1/cv-search', {
        method: 'POST', headers: authHeadersForm, body: formData
      });
      const data = await resp.json();
      hideTyping();
      addMsg('assistant', data.message || 'Here are the matching products.', data.audio_url, data.sources, data.products);
    } catch (err) {
      hideTyping();
      addMsg('assistant', "Failed to process image.");
    }
  }

  // ── Session close ─────────────────────────────────────────────
  window.addEventListener('beforeunload', function () {
    if (msgCount > 0) {
      _authedFetch(API_URL + '/v1/session/close?session_id=' + SESSION_ID, {
        method: 'POST',
        headers: authHeaders,
        keepalive: true
      }).catch(function () { });
    }
  });

  // ── Real-time config sync (polls every 30s) ──────────────────
  var _lastConfigHash = JSON.stringify(cfg);
  setInterval(async function () {
    try {
      var res = await apiFetch(
        companySlug
          ? '/v1/widget/config?slug=' + encodeURIComponent(companySlug)
          : '/v1/widget/config',
        { headers: authHeaders }
      );
      if (!res.ok) return;
      var newConfig = await res.json();
      var newCfg = {
        color1: newConfig.primary_color || '#2952e3',
        color2: newConfig.accent_color || '#00d4f5',
        colorBotText: newConfig.bot_text_color || '#ffffff',
        colorUserText: newConfig.user_text_color || '#ffffff',
        bgColor: newConfig.secondary_color || '#ffffff',
        bgImage: newConfig.bg_image_url || '',
        greeting: newConfig.greeting || cfg.greeting,
        proactiveMsg: newConfig.proactive_message || cfg.proactiveMsg,
        name: newConfig.business_name || cfg.name,
        logoUrl: newConfig.logo_url || '',
        tts: newConfig.tts_enabled !== false,
        stt: newConfig.stt_enabled !== false,
        cv: newConfig.cv_search_enabled !== false,
      };
      var newHash = JSON.stringify(newCfg);
      if (newHash === _lastConfigHash) return;
      _lastConfigHash = newHash;
      // Apply changes
      Object.assign(cfg, newCfg);
      // Bug 4 fix: sync plus button visibility with cv flag
      if (plusBtn) plusBtn.style.display = cfg.cv ? 'flex' : 'none';
      // Update proactive bubble text
      var proTextEl = shadow.getElementById('proText');
      if (proTextEl) proTextEl.textContent = cfg.proactiveMsg;
      // Rebuild CSS with new colors
      styleEl.textContent = styleEl.textContent
        .replace(/background:linear-gradient\(135deg,[^)]+\)/g,
          'background:linear-gradient(135deg,' + cfg.color1 + ',' + cfg.color2 + ')')
        .replace(/background:#[a-fA-F0-9]{6};color:#[a-fA-F0-9]{6};border-radius:4px/,
          'background:' + cfg.color1 + ';color:' + cfg.colorBotText + ';border-radius:4px')
        .replace(/background:#[a-fA-F0-9]{6};color:#[a-fA-F0-9]{6};border-radius:16px 4px/,
          'background:' + cfg.color2 + ';color:' + cfg.colorUserText + ';border-radius:16px 4px');
      var msgsEl = shadow.querySelector('.messages');
      if (msgsEl) {
        msgsEl.style.background = cfg.bgColor;
        if (cfg.bgImage) {
          msgsEl.style.backgroundImage = "url('" + cfg.bgImage + "')";
          msgsEl.style.backgroundSize = "cover";
          msgsEl.style.backgroundPosition = "center";
        } else {
          msgsEl.style.backgroundImage = "none";
        }
      }
      // Update header name
      var nameEl = shadow.querySelector('.agent-name');
      if (nameEl) nameEl.textContent = cfg.name;
      // Update logo
      var hdrAvImg = shadow.querySelector('.agent-av img');
      if (cfg.logoUrl) {
        if (hdrAvImg) { hdrAvImg.src = cfg.logoUrl; }
        else {
          var avEl = shadow.querySelector('.agent-av');
          if (avEl) { avEl.innerHTML = '<img src="' + cfg.logoUrl + '" onerror="this.style.display=\'none\'"/>'; }
        }
        // Also refresh all message avatar images so presigned URLs stay valid
        var allMsgAvImgs = shadow.querySelectorAll('.msg.assistant .msg-av img');
        allMsgAvImgs.forEach(function(img) { img.src = cfg.logoUrl; });
      }
      var launcherImg = shadow.querySelector('.launcher img.l-img');
      if (cfg.logoUrl && launcherImg) { launcherImg.src = cfg.logoUrl; }
      console.log('[LeadsAI] Config refreshed');
    } catch (e) { }
  }, 30000);

})();