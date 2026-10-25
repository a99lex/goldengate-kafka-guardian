let token='',collector='',busy=false,connected=false;
const $=s=>document.querySelector(s), esc=s=>String(s??'Unknown').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const table=(title,columns,rows)=>`<section class="panel lower"><div class="panel-head"><h2>${title}</h2></div><div class="table-wrap"><table><thead><tr>${columns.map(c=>`<th>${esc(c)}</th>`).join('')}</tr></thead><tbody>${rows.length?rows.map(r=>`<tr>${r.map(v=>`<td>${esc(v)}</td>`).join('')}</tr>`).join(''):`<tr><td colspan="${columns.length}">No measurements</td></tr>`}</tbody></table></div></section>`;
async function refresh(){
 if(!connected||busy)return;busy=true;
 const currentToken=token;
 try{
  const r=await fetch('/api/snapshot?collector='+encodeURIComponent(collector),{headers:{Authorization:'Bearer '+token},cache:'no-store',signal:AbortSignal.timeout(15000)});
  const s=await r.json();if(!r.ok)throw Error(s.error||'Request failed');if(!connected||token!==currentToken)return;
  $('#status').textContent=`${s.stale?'STALE — ':'Receiving measurements — '}${s.errors.length?'Partial collection failure — ':''}Collected ${s.collectedAt}; received ${s.receivedAt}.`;
  $('#metrics').innerHTML=table('GoldenGate',['Deployment','Process','Type','Status','Lag (seconds)'],s.processes.map(p=>[p.deployment,p.id,p.kind,p.status,p.lag]))+table('Kafka consumer lag',['Group','Topic','Partition','Committed offset','End offset','Lag (records)'],s.partitions.map(p=>[p.group,p.topic,p.partition,p.committed,p.endOffset,p.lag]))+table('Collection errors',['Source','Error'],s.errors.map(e=>[e.source,e.message]));
 }catch(e){if(connected){$('#status').textContent='UNAVAILABLE — '+e.message;$('#metrics').innerHTML='';}}
 finally{busy=false;}
}
$('#connect').onsubmit=e=>{e.preventDefault();token=$('#token').value;collector=$('#collector').value;connected=true;refresh();};
$('#disconnect').onclick=()=>{connected=false;token='';$('#token').value='';$('#metrics').innerHTML='';$('#status').textContent='Disconnected';};
setInterval(refresh,15000);
