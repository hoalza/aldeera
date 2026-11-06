// خادم الديرة: يقدّم الصفحة ويمرّر حالة الغرف بين اللاعبين (WebSocket). لا يحفظ شيئاً.
const http=require('http'),fs=require('fs'),path=require('path');
const {WebSocketServer}=require('ws');
const PORT=process.env.PORT||3000;
const page=fs.readFileSync(path.join(__dirname,'index.html'));
const srv=http.createServer((q,r)=>{
  if(q.url==='/health'){r.end('ok');return}
  r.writeHead(200,{'content-type':'text/html;charset=utf-8','cache-control':'no-cache'});r.end(page)});
const wss=new WebSocketServer({server:srv,path:'/ws',maxPayload:16*1024});
const rooms=new Map(); // name -> Map(peerId -> {ws,pr})
const dirty=new Set();
let seq=0;
wss.on('connection',ws=>{
  ws.joined=new Map(); // name -> id
  ws.msgs=0;ws.t0=Date.now();
  ws.isAlive=true;ws.on('pong',()=>{ws.isAlive=true});
  ws.on('message',buf=>{
    let m;try{m=JSON.parse(buf)}catch(e){return}
    if(!m||typeof m.name!=='string'||!/^[a-z0-9][a-z0-9_.-]{0,47}$/.test(m.name))return;
    if(m.t==='join'){
      if(ws.joined.has(m.name))return;
      if(ws.joined.size>=2)return;
      let r=rooms.get(m.name);
      if(!r){if(rooms.size>=500)return;r=new Map();rooms.set(m.name,r)}
      if(r.size>=6)return;
      const id=String(++seq).padStart(3,'0')+Math.random().toString(36).slice(2,6);
      // اسم الطرف عند الآخرين: ما أرسله العميل مع ختم الخادم، لمنع الانتحال
      ws.joined.set(m.name,id);r.set(id,{ws,pr:{},client:String(m.peer||'').slice(0,24)});
      ws.send(JSON.stringify({t:'hello',name:m.name,id}));dirty.add(m.name);
    }else if(m.t==='pres'){
      const id=ws.joined.get(m.name),r=rooms.get(m.name);if(!id||!r)return;
      const e=r.get(id);if(!e||!m.p||typeof m.p!=='object')return;
      if(m.full)e.pr={};
      for(const k of Object.keys(m.p).slice(0,16)){if(m.p[k]===null)delete e.pr[k];else e.pr[k]=m.p[k]}
      if(JSON.stringify(e.pr).length>9000){e.pr={}}
      dirty.add(m.name);
    }else if(m.t==='leave')drop(ws,m.name);
  });
  ws.on('close',()=>{for(const n of [...ws.joined.keys()])drop(ws,n)});
});
function drop(ws,name){
  const id=ws.joined.get(name);if(!id)return;ws.joined.delete(name);
  const r=rooms.get(name);if(!r)return;r.delete(id);
  if(!r.size)rooms.delete(name);else dirty.add(name);
}
// كل 40ms نرسل لكل غرفة تغيّرت قائمة اللاعبين وحالاتهم
setInterval(()=>{
  for(const name of dirty){const r=rooms.get(name);if(!r)continue;
    // كل عميل يتعرّف على نفسه بمعرّفه الذي أرسله، فنمرّر معرّف العميل نفسه
    const list=[...r.values()].map(e=>({p:e.client,pr:e.pr}));
    const msg=JSON.stringify({t:'peers',name,peers:list});
    for(const e of r.values())if(e.ws.readyState===1)e.ws.send(msg)}
  dirty.clear();
},40);
setInterval(()=>{for(const ws of wss.clients){if(!ws.isAlive){ws.terminate();continue}ws.isAlive=false;ws.ping()}},25000);
srv.listen(PORT,()=>console.log('الديرة تعمل على المنفذ '+PORT));
