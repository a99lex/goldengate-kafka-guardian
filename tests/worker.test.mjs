import {test} from 'node:test';
import assert from 'node:assert/strict';
import worker,{validate} from '../worker.mjs';
const secret='x'.repeat(40);
const snapshot=()=>({schemaVersion:1,collectorId:'default',collectedAt:new Date().toISOString(),processes:[{id:'EXT1',deployment:'LAB',kind:'Extract',status:'Running',lag:3}],partitions:[{group:'g',topic:'t',partition:0,committed:5,endOffset:10,lag:5}],errors:[]});
const req=(path,method='GET',token=secret,data)=>new Request('https://guardian.example'+path,{method,headers:{Authorization:'Bearer '+token,'Content-Type':'application/json'},...(data?{body:JSON.stringify(data)}:{})});
test('read and ingest tokens are separate; missing secrets fail closed',async()=>{
 const env={INGEST_TOKEN:secret,READ_TOKEN:'r'.repeat(40)};
 assert.equal((await worker.fetch(req('/api/snapshot'),env)).status,401);
 assert.equal((await worker.fetch(req('/api/ingest','POST',secret,snapshot()),{})).status,401);
 assert.equal((await worker.fetch(req('/api/ingest','GET'),env)).status,405);
});
test('reject invalid, stale and oversized payloads',async()=>{
 const env={INGEST_TOKEN:secret,DB:{}};
 const s=snapshot();s.collectedAt='2000-01-01';assert.equal(validate(s),false);
 s.collectedAt=new Date().toISOString();s.processes[0].lag=-1;assert.equal(validate(s),false);
 assert.equal((await worker.fetch(req('/api/ingest','POST',secret,{bad:true}),env)).status,400);
 assert.equal((await worker.fetch(req('/api/ingest','POST',secret,{value:'a'.repeat(513000)}),env)).status,413);
});
test('ingest and read roundtrip, stale status and empty source',async()=>{
 let stored;
 const DB={prepare(sql){return {bind(...args){return {...this,args,sql};},async all(){return {results:stored?[{payload:stored,received_at:new Date().toISOString()}]:[]};}};},async batch(statements){stored=statements[0].args[3];}};
 const env={DB,INGEST_TOKEN:secret,READ_TOKEN:secret};
 assert.equal((await worker.fetch(req('/api/snapshot'),env)).status,404);
 assert.equal((await worker.fetch(req('/api/ingest','POST',secret,snapshot()),env)).status,202);
 const r=await (await worker.fetch(req('/api/snapshot'),env)).json();assert.equal(r.processes[0].lag,3);assert.equal(r.stale,false);
 const s=JSON.parse(stored);s.collectedAt=new Date(Date.now()-180000).toISOString();stored=JSON.stringify(s);
 assert.equal((await (await worker.fetch(req('/api/snapshot'),env)).json()).stale,true);
});
