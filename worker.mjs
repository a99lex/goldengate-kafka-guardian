const headers={'Content-Type':'application/json','Cache-Control':'no-store','X-Content-Type-Options':'nosniff'};
const json=(data,status=200)=>new Response(JSON.stringify(data),{status,headers});
async function authorized(request, secret){
 if(!secret || secret.length<32)return false;
 const supplied=request.headers.get('Authorization')||'';
 const hash=async s=>new Uint8Array(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(s)));
 const a=await hash(supplied),b=await hash('Bearer '+secret);
 return a.reduce((n,v,i)=>n|(v^b[i]),0)===0;
}
export function validate(s){
 if(!s||s.schemaVersion!==1||typeof s.collectorId!=='string'||!/^[-\w]{1,64}$/.test(s.collectorId))return false;
 if(!Number.isFinite(Date.parse(s.collectedAt))||Math.abs(Date.now()-Date.parse(s.collectedAt))>300000)return false;
 if(!Array.isArray(s.processes)||!Array.isArray(s.partitions)||!Array.isArray(s.errors)||s.processes.length>1000||s.partitions.length>10000)return false;
 const str=x=>typeof x==='string'&&x.length<=256;
 const metric=x=>x===null||(typeof x==='number'&&Number.isFinite(x)&&x>=0);
 return s.processes.every(p=>p&&str(p.id)&&str(p.deployment)&&['Extract','Replicat'].includes(p.kind)&&str(p.status)&&metric(p.lag)) && s.partitions.every(p=>p&&str(p.topic)&&str(p.group)&&Number.isInteger(p.partition)&&p.partition>=0&&metric(p.lag)&&metric(p.committed)&&metric(p.endOffset)) && s.errors.every(e=>e&&str(e.source)&&str(e.message));
}
export default {async fetch(request,env){
 const path=new URL(request.url).pathname;
 if(!path.startsWith('/api/'))return env.ASSETS.fetch(request);
 if(path==='/api/health')return json({service:'Guardian',version:'0.2.0'});
 if(!['/api/ingest','/api/snapshot','/api/history'].includes(path))return json({error:'Not found'},404);
 const ingest=path==='/api/ingest';
 if(!await authorized(request,ingest?env.INGEST_TOKEN:env.READ_TOKEN))return json({error:'Unauthorized'},401);
 if(request.method!==(ingest?'POST':'GET'))return json({error:'Method not allowed'},405);
 if(!env.DB)return json({error:'D1 binding DB not configured'},503);
 try{
  if(ingest){
   if(!request.headers.get('content-type')?.includes('application/json'))return json({error:'Expected application/json'},415);
   const reader=request.body?.getReader();let total=0,chunks=[];
   if(reader)while(true){const {done,value}=await reader.read();if(done)break;total+=value.byteLength;if(total>512000){await reader.cancel();return json({error:'Payload too large'},413);}chunks.push(value);}
   const body=new Uint8Array(total);let offset=0;for(const c of chunks){body.set(c,offset);offset+=c.length;}
   let s;try{s=JSON.parse(new TextDecoder().decode(body));}catch{return json({error:'Invalid JSON'},400);}
   if(!validate(s))return json({error:'Invalid snapshot or timestamp'},400);
   const now=new Date().toISOString();
   await env.DB.batch([
    env.DB.prepare('INSERT INTO snapshots(collector_id,collected_at,received_at,payload) VALUES(?,?,?,?)').bind(s.collectorId,s.collectedAt,now,JSON.stringify(s)),
    env.DB.prepare("DELETE FROM snapshots WHERE julianday(received_at) < julianday('now','-7 days')")
   ]);
   return json({accepted:true,receivedAt:now},202);
  }
  const id=new URL(request.url).searchParams.get('collector')||'default';
  const query=env.DB.prepare('SELECT payload,received_at FROM snapshots WHERE collector_id=? ORDER BY id DESC LIMIT ?').bind(id,path==='/api/history'?24:1);
  const {results}=await query.all();
  if(!results.length)return json({error:'No measurements received yet'},404);
  const rows=results.map(r=>({...JSON.parse(r.payload),receivedAt:r.received_at,stale:Date.now()-Date.parse(JSON.parse(r.payload).collectedAt)>120000}));
  return json(path==='/api/history'?rows:rows[0]);
 }catch{return json({error:'Storage operation failed; check D1 migration and binding'},503);}
}};
