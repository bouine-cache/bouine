package dashboard

// allTemplates is the full HTML template set for the dashboard.
// Loaded once at startup via template.Must(template.New("").Funcs(...).Parse(allTemplates)).
//
// Layout is split into "layout-head" (open) and "layout-foot" (close) to
// avoid duplicate {{define}} names — html/template does not support template
// inheritance via multiple same-named blocks across definitions.
// Each page template calls {{template "layout-head" .}} … content … scripts …
// {{template "layout-foot" .}} and relies on data fields .Page, .PageTitle,
// .NodeName for active-nav and header rendering.
const allTemplates = `
{{define "layout-head"}}<!DOCTYPE html>
<html lang="en" data-theme="dark">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>bouine · {{.PageTitle}}</title>
<script src="https://unpkg.com/htmx.org@2.0.3" crossorigin="anonymous"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&family=Inter:wght@400;500;600;700&display=swap');
*{box-sizing:border-box;margin:0;padding:0}
html[data-theme="dark"]{--bg:#08060f;--s:#0e0b18;--card:#110e1c;--b:#1e1830;--t:#ede6ff;--m:#6050a0;--a:#c4b5fd;--a2:#8b5cf6;--a3:#6d28d9;--g:#34d399;--r:#fb7185;--y:#fbbf24;--pg:rgba(139,92,246,.5)}
html[data-theme="light"]{--bg:#f8f5ff;--s:#fff;--card:#fdfbff;--b:#e4d9ff;--t:#180840;--m:#8b7ab8;--a:#5b21b6;--a2:#7c3aed;--a3:#a78bfa;--g:#059669;--r:#e11d48;--y:#b45309;--pg:rgba(91,33,182,.2)}
body{font-family:'Inter',system-ui,sans-serif;background:var(--bg);color:var(--t);min-height:100vh;display:flex;font-size:13px;transition:background .25s,color .25s;position:relative;overflow:hidden}
aside{width:180px;background:var(--s);border-right:1px solid var(--b);padding:0;display:flex;flex-direction:column;flex-shrink:0;position:relative;z-index:1;height:100vh}
.logo{padding:.85rem 1rem;border-bottom:1px solid var(--b);display:flex;align-items:center;gap:.55rem}
.logo-gem{width:28px;height:28px;border-radius:7px;background:linear-gradient(135deg,var(--a3),var(--a2));display:flex;align-items:center;justify-content:center;font-size:.8rem;flex-shrink:0}
html[data-theme="dark"] .logo-gem{box-shadow:0 0 10px var(--pg)}
.logo-name{font-family:'JetBrains Mono',monospace;font-weight:700;font-size:.82rem;letter-spacing:.04em;color:var(--a)}
.logo-v{font-size:.6rem;color:var(--m);margin-top:.1rem}
nav{padding:.5rem 0;flex:1}
.ng{padding:.4rem .75rem .15rem;font-size:.58rem;text-transform:uppercase;letter-spacing:.14em;color:var(--m);opacity:.7}
nav a{display:flex;align-items:center;gap:.5rem;padding:.45rem .75rem;font-size:.76rem;font-weight:500;color:var(--m);text-decoration:none;transition:all .15s;margin:0 .35rem .1rem;border-radius:5px}
nav a.active{background:rgba(139,92,246,.12);color:var(--a)}
html[data-theme="dark"] nav a.active{box-shadow:inset 1px 0 0 var(--a2)}
nav a:hover:not(.active){background:rgba(139,92,246,.05);color:var(--t)}
.ni{font-size:.8rem;width:16px;text-align:center;flex-shrink:0}
.sbf{padding:.65rem;border-top:1px solid var(--b);font-family:'JetBrains Mono',monospace;font-size:.62rem}
.sbf .sl{display:flex;justify-content:space-between;margin-bottom:.2rem}
.sbf .sk{color:var(--m)}.sbf .sv{color:var(--a)}
main{flex:1;display:flex;flex-direction:column;overflow:hidden;position:relative;z-index:1;height:100vh}
.hdr{height:40px;background:var(--s);border-bottom:1px solid var(--b);display:flex;align-items:center;padding:0 1rem;gap:.85rem;flex-shrink:0}
.brand{font-family:'JetBrains Mono',monospace;font-weight:700;font-size:.82rem;color:var(--a);letter-spacing:.04em}
.ph{font-size:.82rem;font-weight:600;color:var(--t)}
.hdr-r{margin-left:auto;display:flex;gap:.5rem;align-items:center}
.pill{display:flex;align-items:center;gap:.3rem;font-size:.7rem;color:var(--m);padding:.18rem .5rem;border-radius:999px;border:1px solid var(--b);font-family:'JetBrains Mono',monospace}
.dot-g{width:6px;height:6px;border-radius:50%;background:var(--g)}
html[data-theme="dark"] .dot-g{box-shadow:0 0 4px var(--g)}
.tabs{height:36px;border-bottom:1px solid var(--b);background:var(--s);display:flex;align-items:stretch;padding:0 1rem;gap:.15rem;flex-shrink:0}
.tabs a{display:flex;align-items:center;font-size:.75rem;color:var(--m);text-decoration:none;padding:0 .65rem;border-bottom:2px solid transparent;transition:all .15s;font-weight:500}
.tabs a.active{color:var(--a);border-bottom-color:var(--a2)}
.tabs a:hover:not(.active){color:var(--t)}
.body{padding:.6rem .75rem;flex:1;overflow:auto;display:flex;flex-direction:column}
.r4{display:grid;grid-template-columns:repeat(4,1fr);gap:.5rem;margin-bottom:.5rem;flex-shrink:0}
.mc{background:var(--card);border:1px solid var(--b);border-radius:8px;padding:.6rem .75rem;position:relative;overflow:hidden}
html[data-theme="dark"] .mc{box-shadow:0 1px 12px rgba(0,0,0,.4)}
.mc::after{content:'';position:absolute;bottom:0;left:0;right:0;height:1px;background:linear-gradient(90deg,transparent,var(--a2),transparent);opacity:.3}
.mk{font-size:.6rem;text-transform:uppercase;letter-spacing:.1em;color:var(--m);margin-bottom:.4rem;font-weight:600}
.mv{font-family:'JetBrains Mono',monospace;font-size:1.35rem;font-weight:700;letter-spacing:-.02em;line-height:1;margin-bottom:.15rem;color:var(--a)}
html[data-theme="dark"] .mv{text-shadow:0 0 16px rgba(139,92,246,.3)}
.md{font-size:.67rem;color:var(--m)}
.good{color:var(--g)}.bad{color:var(--r)}
.r21{display:grid;grid-template-columns:2fr 1fr;gap:.5rem;margin-bottom:.5rem;flex:1;min-height:0}
.cc{background:var(--card);border:1px solid var(--b);border-radius:8px;padding:.7rem;display:flex;flex-direction:column;min-height:0}
html[data-theme="dark"] .cc{box-shadow:0 1px 12px rgba(0,0,0,.4)}
.ct{font-size:.6rem;font-weight:600;text-transform:uppercase;letter-spacing:.1em;color:var(--m);margin-bottom:.4rem;font-family:'JetBrains Mono',monospace;flex-shrink:0}
.ch{flex:1;min-height:80px;position:relative}
.tc{background:var(--card);border:1px solid var(--b);border-radius:8px;overflow:hidden;margin-bottom:.5rem;flex-shrink:0}
html[data-theme="dark"] .tc{box-shadow:0 1px 12px rgba(0,0,0,.4)}
.th{padding:.55rem .85rem;border-bottom:1px solid var(--b);display:flex;justify-content:space-between;align-items:center;background:rgba(139,92,246,.03)}
.tl{font-size:.6rem;font-weight:700;text-transform:uppercase;letter-spacing:.1em;color:var(--m);font-family:'JetBrains Mono',monospace}
table{width:100%;border-collapse:collapse}
th{padding:.35rem .85rem;font-size:.6rem;font-weight:600;text-transform:uppercase;letter-spacing:.08em;color:var(--m);text-align:left;border-bottom:1px solid var(--b);cursor:pointer;white-space:nowrap}
th:hover{color:var(--a)}
td{padding:.45rem .85rem;font-size:.77rem;border-bottom:1px solid rgba(139,92,246,.05)}
html[data-theme="light"] td{border-bottom:1px solid rgba(109,40,217,.04)}
tr:last-child td{border:none}
tr:hover td{background:rgba(139,92,246,.04)}
td:first-child{font-family:'JetBrains Mono',monospace;font-size:.7rem;color:var(--a)}
.lz{display:inline-flex;align-items:center;gap:.2rem;padding:.1rem .42rem;border-radius:999px;font-size:.63rem;font-weight:700;font-family:'JetBrains Mono',monospace}
.lz::before{content:'●';font-size:.45rem}
.lz.g{background:rgba(52,211,153,.1);color:var(--g)}
.lz.y{background:rgba(251,191,36,.1);color:var(--y)}
.lz.d{background:rgba(139,92,246,.07);color:var(--m)}
.lz.r{background:rgba(251,113,133,.1);color:var(--r)}
.r3{display:grid;grid-template-columns:repeat(3,1fr);gap:.5rem;flex-shrink:0}
.r2{display:grid;grid-template-columns:1fr 1fr;gap:.7rem}
.bc{background:var(--card);border:1px solid var(--b);border-radius:8px;padding:.65rem .75rem}
html[data-theme="dark"] .bc{box-shadow:0 1px 12px rgba(0,0,0,.4)}
.bc-t{font-size:.6rem;font-weight:700;text-transform:uppercase;letter-spacing:.1em;color:var(--m);margin-bottom:.45rem;font-family:'JetBrains Mono',monospace}
.pr{display:flex;align-items:center;justify-content:space-between;padding:.28rem 0;border-bottom:1px solid var(--b)}
.pr:last-child{border:none}
.pn{font-family:'JetBrains Mono',monospace;font-size:.7rem;color:var(--a)}
.pa{font-size:.6rem;color:var(--m);margin-top:.08rem}
.pd{width:6px;height:6px;border-radius:50%;background:var(--g)}
html[data-theme="dark"] .pd{box-shadow:0 0 5px var(--g)}
.pd-stale{background:var(--y);box-shadow:none}
.stat-row{display:flex;justify-content:space-between;align-items:baseline;padding:.22rem 0;border-bottom:1px solid var(--b);font-size:.72rem}
.stat-row:last-child{border:none}
.stat-k{color:var(--m);font-size:.7rem}
.stat-v{font-family:'JetBrains Mono',monospace;color:var(--a);font-weight:600}
.fgroup{margin-bottom:.75rem}
.fgroup label{display:block;font-size:.62rem;font-weight:600;text-transform:uppercase;letter-spacing:.1em;color:var(--m);margin-bottom:.3rem}
.finput{width:100%;background:rgba(139,92,246,.04);border:1px solid var(--b);color:var(--t);padding:.38rem .55rem;border-radius:5px;font-size:.76rem;font-family:inherit}
.finput:focus{outline:none;border-color:rgba(139,92,246,.4);box-shadow:0 0 0 2px rgba(139,92,246,.1)}
.frow{display:flex;gap:.35rem}
.frow .finput{flex:1}
.btn{padding:.35rem .65rem;border-radius:5px;border:none;font-size:.7rem;font-weight:600;cursor:pointer;font-family:inherit;transition:all .15s;white-space:nowrap}
.bp{background:var(--a2);color:#fff}.bp:hover{background:var(--a)}
.bo{background:rgba(139,92,246,.06);border:1px solid var(--b);color:var(--m)}.bo:hover{border-color:var(--a);color:var(--a)}
.bg2{background:rgba(52,211,153,.08);border:1px solid rgba(52,211,153,.25);color:var(--g)}
.confirm-box{display:none;background:rgba(139,92,246,.08);border:1px solid rgba(139,92,246,.3);border-radius:8px;padding:.85rem;margin-top:.6rem}
.confirm-box.show{display:block}
.confirm-box p{font-size:.74rem;color:var(--t);margin-bottom:.5rem}
.flash-ok{font-size:.74rem;color:var(--g);padding:.5rem .75rem;background:rgba(52,211,153,.08);border:1px solid rgba(52,211,153,.25);border-radius:6px}
.htmx-indicator{opacity:0;transition:opacity .2s}
.htmx-request .htmx-indicator{opacity:1}
.htmx-request.htmx-indicator{opacity:1}
.pg-hd{display:flex;align-items:center;justify-content:space-between;margin-bottom:.75rem;flex-shrink:0}
.pg-hd h2{font-size:.95rem;font-weight:700}
.pg-hd .sub{font-size:.7rem;color:var(--m)}
.search-bar{background:rgba(139,92,246,.04);border:1px solid var(--b);color:var(--t);padding:.35rem .55rem;border-radius:5px;font-size:.75rem;font-family:inherit;width:200px}
.search-bar:focus{outline:none;border-color:rgba(139,92,246,.4)}
.code-block{background:var(--bg);border:1px solid var(--b);border-radius:6px;padding:.75rem;font-family:'JetBrains Mono',monospace;font-size:.7rem;line-height:1.7;color:var(--t);overflow:auto;margin-bottom:.7rem}
#bouine-tint{position:fixed;inset:0;z-index:0;pointer-events:none;background:radial-gradient(ellipse 120% 80% at 20% 20%,rgba(109,40,217,.25) 0%,transparent 55%),radial-gradient(ellipse 100% 90% at 80% 80%,rgba(139,92,246,.2) 0%,transparent 55%),radial-gradient(ellipse 80% 60% at 50% 50%,rgba(91,33,182,.12) 0%,transparent 60%)}
#bouine-noise{position:fixed;inset:0;width:100%;height:100%;pointer-events:none;z-index:0}
</style>
</head>
<body>
<div id="bouine-tint"></div>
<canvas id="bouine-noise"></canvas>
<aside>
  <div class="logo">
    <div class="logo-gem">🐟</div>
    <div><div class="logo-name">bouine</div><div class="logo-v">v1.0-rc</div></div>
  </div>
  <nav>
    <div class="ng">Monitor</div>
    <a href="/dashboard/" {{if eq .Page "overview"}}class="active"{{end}}><span class="ni">◈</span>Overview</a>
    <a href="/dashboard/routes" {{if eq .Page "routes"}}class="active"{{end}}><span class="ni">⬡</span>Routes</a>
    <a href="/dashboard/cluster" {{if eq .Page "cluster"}}class="active"{{end}}><span class="ni">◎</span>Cluster</a>
    <div class="ng">Operate</div>
    <a href="/dashboard/invalidation" {{if eq .Page "invalidation"}}class="active"{{end}}><span class="ni">⌦</span>Invalidation</a>
    <a href="/dashboard/config" {{if eq .Page "config"}}class="active"{{end}}><span class="ni">⚙</span>Config</a>
  </nav>
  <div class="sbf">
    <div class="sl"><span class="sk">node</span><span class="sv">{{.NodeName}}</span></div>
  </div>
</aside>
<main>
  <div class="hdr">
    <span class="brand">bouine</span>
    <span class="ph">{{.PageTitle}}</span>
    <div class="hdr-r">
      <div class="pill"><span class="dot-g"></span>dashboard</div>
      <button style="background:transparent;border:1px solid var(--b);color:var(--m);font-size:.67rem;padding:.2rem .5rem;border-radius:4px;cursor:pointer;font-family:'JetBrains Mono',monospace" onclick="(function(b){var h=document.documentElement,d=h.getAttribute('data-theme');h.setAttribute('data-theme',d==='light'?'dark':'light');b.textContent=h.getAttribute('data-theme')==='light'?'🌙':'☀';})(this)">☀</button>
    </div>
  </div>
  <div class="tabs">
    <a href="/dashboard/" {{if eq .Page "overview"}}class="active"{{end}}>Overview</a>
    <a href="/dashboard/routes" {{if eq .Page "routes"}}class="active"{{end}}>Routes</a>
    <a href="/dashboard/cluster" {{if eq .Page "cluster"}}class="active"{{end}}>Cluster</a>
    <a href="/dashboard/invalidation" {{if eq .Page "invalidation"}}class="active"{{end}}>Invalidation</a>
    <a href="/dashboard/config" {{if eq .Page "config"}}class="active"{{end}}>Config</a>
  </div>
  <div class="body">
{{end}}

{{define "layout-foot"}}
  </div>
</main>
<script>
(function(){
  var c=document.getElementById('bouine-noise');
  if(!c)return;var gl=c.getContext('webgl');if(!gl)return;
  var V='attribute vec2 a;void main(){gl_Position=vec4(a,0.,1.);}';
  var F=['precision mediump float;','uniform float t;uniform vec2 r;','float hash(vec2 p){vec3 p3=fract(vec3(p.xyx)*.1031);p3+=dot(p3,p3.yzx+33.33);return fract((p3.x+p3.y)*p3.z);}','void main(){','vec2 co=gl_FragCoord.xy+vec2(sin(t*.1)*.5,cos(t*.13)*.5);','vec2 s=co+t*.01;','float u1=max(hash(s),.0001);float u2=hash(s+vec2(1.,0.));','float n=sqrt(-2.*log(u1))*cos(6.28318*u2);','gl_FragColor=vec4(vec3(clamp(n*.08+.5,0.,1.)),1.);}'].join('');
  function sh(tp,sr){var s=gl.createShader(tp);gl.shaderSource(s,sr);gl.compileShader(s);return s;}
  var p=gl.createProgram();gl.attachShader(p,sh(gl.VERTEX_SHADER,V));gl.attachShader(p,sh(gl.FRAGMENT_SHADER,F));gl.linkProgram(p);
  var buf=gl.createBuffer();gl.bindBuffer(gl.ARRAY_BUFFER,buf);gl.bufferData(gl.ARRAY_BUFFER,new Float32Array([-1,-1,1,-1,-1,1,-1,1,1,-1,1,1]),gl.STATIC_DRAW);
  var tl=gl.getUniformLocation(p,'t'),rl=gl.getUniformLocation(p,'r'),al=gl.getAttribLocation(p,'a'),last=0;
  function ub(){var dark=document.documentElement.getAttribute('data-theme')!=='light';c.style.mixBlendMode='overlay';c.style.opacity=dark?'0.45':'0.15';}
  function resize(){c.width=innerWidth;c.height=innerHeight;gl.viewport(0,0,c.width,c.height);}
  function frame(tm){if(tm-last>=83){resize();ub();gl.useProgram(p);gl.uniform1f(tl,tm*.001);gl.uniform2f(rl,c.width,c.height);gl.enableVertexAttribArray(al);gl.bindBuffer(gl.ARRAY_BUFFER,buf);gl.vertexAttribPointer(al,2,gl.FLOAT,false,0,0);gl.drawArrays(gl.TRIANGLES,0,6);last=tm;}requestAnimationFrame(frame);}
  window.addEventListener('resize',resize);requestAnimationFrame(frame);
})();
</script>
<script>
function sortTable(id,col,th){var t=document.getElementById(id),tb=t.querySelector('tbody');var rows=Array.from(tb.querySelectorAll('tr'));var asc=th.dataset.asc==='1';t.querySelectorAll('th').forEach(function(h){delete h.dataset.asc;});th.dataset.asc=asc?'0':'1';rows.sort(function(a,b){var ac=a.querySelectorAll('td')[col],bc=b.querySelectorAll('td')[col];var av=ac.dataset.val!==undefined?parseFloat(ac.dataset.val):ac.textContent.trim();var bv=bc.dataset.val!==undefined?parseFloat(bc.dataset.val):bc.textContent.trim();if(typeof av==='number')return asc?av-bv:bv-av;return asc?av.localeCompare(bv):bv.localeCompare(av);});rows.forEach(function(r){tb.appendChild(r);});}
function filterTable(input,tableId){var q=input.value.toLowerCase();document.querySelector('#'+tableId+' tbody').querySelectorAll('tr').forEach(function(r){r.style.display=r.textContent.toLowerCase().includes(q)?'':'none';});}
</script>
</body>
</html>
{{end}}

{{/* ══ OVERVIEW ══ */}}
{{define "overview"}}
{{template "layout-head" .}}
<div class="r4" hx-get="/dashboard/" hx-trigger="every 5s" hx-swap="outerHTML" hx-select=".r4">
  <div class="mc"><div class="mk">Requests / s</div><div class="mv">{{fmtReqS .ReqPerSec}}</div><div class="md">last 60s</div></div>
  <div class="mc"><div class="mk">Hit ratio</div><div class="mv">{{fmtHitPct .HitPct}}</div><div class="md">last 60s</div></div>
  <div class="mc"><div class="mk">p99 latency</div><div class="mv">{{fmtLatMs .P99MS}}</div><div class="md">last 60s</div></div>
  <div class="mc"><div class="mk">Error rate</div><div class="mv">{{fmtHitPct .ErrPct}}</div><div class="md">last 60s</div></div>
</div>
<div class="r21">
  <div class="cc"><div class="ct">throughput (last 6 min)</div><div class="ch"><canvas id="c-req"></canvas></div></div>
  <div class="cc"><div class="ct">cache split</div><div class="ch"><canvas id="c-split"></canvas></div>
</div>
</div>
<div class="r3">
  <div class="bc"><div class="bc-t">Cluster peers</div>
    {{range .PeerResults}}
    <div class="pr"><div><div class="pn">{{.NodeName}}</div><div class="pa">{{if .Stale}}stale{{else}}live{{end}}</div></div>
      <span class="pd{{if .Stale}} pd-stale{{end}}"></span></div>
    {{else}}<div style="font-size:.72rem;color:var(--m)">Single-node mode</div>{{end}}
  </div>
  <div class="bc" hx-get="/dashboard/" hx-trigger="every 10s" hx-swap="outerHTML" hx-select=".r3 .bc:nth-child(2)">
    <div class="bc-t">Top routes</div>
    {{range .RouteStats}}
    <div class="stat-row"><span class="stat-k">{{.Route}}</span><span class="stat-v">{{fmtHitPct .HitPct}}</span></div>
    {{else}}<div style="font-size:.7rem;color:var(--m)">No data yet</div>{{end}}
  </div>
  <div class="bc">
    <div class="bc-t">Quick purge</div>
    <div class="fgroup"><label>URL</label>
      <div class="frow" hx-target="#purge-result" hx-swap="innerHTML">
        <input class="finput" name="url" placeholder="https://example.com/path" id="qp-url">
        <button class="btn bp" hx-post="/v1/purge" hx-vals='js:{"url": document.getElementById("qp-url").value}' hx-headers='{"Authorization": "Bearer {{.Token}}"}'>Purge</button>
      </div>
    </div>
    <div id="purge-result" style="font-size:.7rem;color:var(--m)"></div>
  </div>
</div>
<script>
(function(){
  var d=document.documentElement.getAttribute('data-theme')!=='light';
  var labels={{toJSON .ChartLabels}};
  var reqs={{toJSON .ChartReqs}};
  var hits={{toJSON .ChartHits}};
  var lastReq=reqs.length>0?reqs[reqs.length-1]:0;
  var lastHit=hits.length>0?hits[hits.length-1]:0;
  var gc={color:d?'rgba(196,181,253,.05)':'rgba(91,33,182,.04)'};
  var tc={color:d?'#6050a0':'#9ca3af',font:{size:9}};
  new Chart(document.getElementById('c-req'),{type:'line',data:{labels:labels,datasets:[
    {label:'req/s',data:reqs,borderColor:d?'#c4b5fd':'#5b21b6',backgroundColor:d?'rgba(196,181,253,.07)':'rgba(91,33,182,.05)',fill:true,tension:.4,pointRadius:0,borderWidth:1.5},
    {label:'hits',data:hits,borderColor:d?'#34d399':'#059669',backgroundColor:'transparent',tension:.4,pointRadius:0,borderWidth:1},
  ]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{labels:{color:d?'#6050a0':'#8b7ab8',font:{size:9}}}},scales:{x:{grid:gc,ticks:{color:tc.color,font:tc.font,maxTicksLimit:8}},y:{grid:gc,ticks:{color:tc.color,font:tc.font}}}}});
  new Chart(document.getElementById('c-split'),{type:'doughnut',data:{labels:['HIT','OTHER'],datasets:[{data:[lastHit,Math.max(0,lastReq-lastHit)],backgroundColor:d?['#c4b5fd','#1e1830']:['#5b21b6','#e4d9ff'],borderWidth:0}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{position:'bottom',labels:{color:d?'#6050a0':'#8b7ab8',font:{size:9},padding:6}}},cutout:'70%'}});
})();
</script>
{{template "layout-foot" .}}
{{end}}

{{/* ══ ROUTES ══ */}}
{{define "routes"}}
{{template "layout-head" .}}
<div class="pg-hd">
  <div><h2>Route performance</h2><div class="sub">cluster-wide aggregated · polled every 10s</div></div>
  <input class="search-bar" placeholder="Filter…" oninput="filterTable(this,'route-tbl')">
</div>
<div class="tc" hx-get="/dashboard/routes" hx-trigger="every 10s" hx-swap="outerHTML" hx-select=".tc">
  <div class="th"><span class="tl">Routes</span><span style="font-size:.62rem;color:var(--m);font-family:'JetBrains Mono',monospace">{{len .RouteStats}} routes</span></div>
  <table id="route-tbl">
    <thead><tr>
      <th onclick="sortTable('route-tbl',0,this)">Prefix ↕</th>
      <th onclick="sortTable('route-tbl',1,this)">Requests ↕</th>
      <th onclick="sortTable('route-tbl',2,this)">Hits ↕</th>
      <th onclick="sortTable('route-tbl',3,this)">Hit % ↕</th>
    </tr></thead>
    <tbody>
      {{range .RouteStats}}
      <tr>
        <td>{{.Route}}</td>
        <td data-val="{{.Requests}}">{{.Requests}}</td>
        <td data-val="{{.Hits}}">{{.Hits}}</td>
        <td data-val="{{.HitPct}}">
          {{if ge .HitPct 80.0}}<span class="lz g">{{fmtHitPct .HitPct}}</span>
          {{else if ge .HitPct 50.0}}<span class="lz y">{{fmtHitPct .HitPct}}</span>
          {{else}}<span class="lz r">{{fmtHitPct .HitPct}}</span>{{end}}
        </td>
      </tr>
      {{else}}<tr><td colspan="4" style="color:var(--m);text-align:center;padding:1rem">No data yet — traffic will populate this table.</td></tr>{{end}}
    </tbody>
  </table>
</div>
{{template "layout-foot" .}}
{{end}}

{{/* ══ CLUSTER ══ */}}
{{define "cluster"}}
{{template "layout-head" .}}
<div class="pg-hd"><div><h2>Cluster peers</h2><div class="sub">fan-out aggregated · timeout 200ms</div></div></div>
<div class="tc" style="margin-bottom:.7rem" hx-get="/dashboard/cluster" hx-trigger="every 10s" hx-swap="outerHTML" hx-select=".tc">
  <div class="th"><span class="tl">Live peers</span><span style="font-size:.62rem;color:var(--m);font-family:'JetBrains Mono',monospace">{{len .PeerResults}} nodes</span></div>
  <table>
    <thead><tr><th>Node</th><th>Status</th><th>Routes (30min)</th><th>Hit %</th><th>p99</th></tr></thead>
    <tbody>
      {{range .PeerResults}}
      <tr>
        <td>{{.NodeName}}</td>
        <td>{{if .Stale}}<span class="lz y">stale</span>{{else}}<span class="lz g">live</span>{{end}}</td>
        <td>{{len .Summary.RouteStats}} routes</td>
        <td>—</td><td>—</td>
      </tr>
      {{else}}<tr><td colspan="5" style="color:var(--m);padding:1rem;text-align:center">Single-node mode</td></tr>{{end}}
    </tbody>
  </table>
</div>
{{template "layout-foot" .}}
{{end}}

{{/* ══ INVALIDATION ══ */}}
{{define "invalidation"}}
{{template "layout-head" .}}
<div class="pg-hd"><div><h2>Cache invalidation</h2><div class="sub">purge · ban · refresh — broadcast to all cluster peers</div></div></div>
<div class="r3">
  <div class="bc">
    <div class="bc-t">Purge — exact URL</div>
    <div class="fgroup"><label>URL</label>
      <div class="frow">
        <input class="finput" name="url" placeholder="https://example.com/products/42" id="purge-url">
        <button class="btn bp" hx-post="/v1/purge" hx-vals='js:{"url":document.getElementById("purge-url").value}' hx-headers='{"Authorization":"Bearer {{.Token}}"}' hx-target="#purge-resp" hx-swap="innerHTML">Purge</button>
      </div>
    </div>
    <div id="purge-resp" style="font-size:.7rem;color:var(--m)"></div>
    <div style="font-size:.7rem;color:var(--m);line-height:1.5;margin-top:.5rem">Removes exact cache key. In cluster, forwarded to key owner. All Vary variants purged.</div>
  </div>
  <div class="bc">
    <div class="bc-t">Ban — predicate</div>
    <div class="fgroup"><label>Host regex</label><input class="finput" placeholder="example\.com" id="ban-host"></div>
    <div class="fgroup"><label>Path regex</label><input class="finput" placeholder="^/api/v1/" id="ban-path"></div>
    <button class="btn" style="background:rgba(251,191,36,.1);border:1px solid rgba(251,191,36,.25);color:var(--y)"
      hx-post="/v1/ban"
      hx-vals='js:{"host_regex":document.getElementById("ban-host").value,"path_regex":document.getElementById("ban-path").value}'
      hx-headers='{"Authorization":"Bearer {{.Token}}"}'
      hx-target="#ban-resp" hx-swap="innerHTML">Issue ban</button>
    <div id="ban-resp" style="font-size:.7rem;color:var(--m);margin-top:.4rem"></div>
  </div>
  <div class="bc">
    <div class="bc-t">Refresh — soft-purge</div>
    <div class="fgroup"><label>URL</label>
      <div class="frow">
        <input class="finput" placeholder="https://example.com/page" id="refresh-url">
        <button class="btn bg2" hx-post="/v1/refresh" hx-vals='js:{"url":document.getElementById("refresh-url").value}' hx-headers='{"Authorization":"Bearer {{.Token}}"}' hx-target="#refresh-resp" hx-swap="innerHTML">Refresh</button>
      </div>
    </div>
    <div id="refresh-resp" style="font-size:.7rem;color:var(--m)"></div>
    <div style="font-size:.7rem;color:var(--m);line-height:1.5;margin-top:.5rem">Marks stale. Next request revalidates with <code style="font-family:'JetBrains Mono',monospace;font-size:.68rem">If-None-Match</code>. If origin returns 304, cached body reused.</div>
  </div>
</div>
{{template "layout-foot" .}}
{{end}}

{{/* ══ CONFIG ══ */}}
{{define "config"}}
{{template "layout-head" .}}
<div class="pg-hd"><div><h2>Config reload</h2><div class="sub">validate → confirm → apply</div></div></div>
<div class="r2">
  <div class="bc">
    <div class="bc-t">Runtime state</div>
    <div class="stat-row"><span class="stat-k">node</span><span class="stat-v">{{.NodeName}}</span></div>
    <div class="stat-row"><span class="stat-k">snapshot path</span><span class="stat-v" style="font-size:.65rem">{{if .SnapshotPath}}{{.SnapshotPath}}{{else}}disabled{{end}}</span></div>
    {{if .Flash}}<div class="flash-ok" style="margin-top:.75rem">{{.Flash}}</div>{{end}}
  </div>
  <div class="bc">
    <div class="bc-t">Reload config</div>
    <div style="font-size:.74rem;color:var(--m);line-height:1.7;margin-bottom:.85rem">
      Reads config from disk, validates fully, and only applies on success.
      The running configuration is <strong style="color:var(--t)">never affected</strong> by parse errors.
    </div>
    <div id="reload-section">
      <button class="btn bp" style="width:100%" hx-post="/dashboard/config/reload" hx-target="#reload-section" hx-swap="innerHTML">↻ Reload config</button>
    </div>
    <div style="font-size:.68rem;color:var(--m);margin-top:.5rem">
      Hot-reloadable: routes, pools, TTLs, TLS certs.<br>
      Not reloadable: listen addresses, storage, cluster settings.
    </div>
  </div>
</div>
{{template "layout-foot" .}}
{{end}}
`
