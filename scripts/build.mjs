import {mkdir,copyFile} from 'node:fs/promises';
await mkdir('public',{recursive:true});
for(const file of ['index.html','live.js','demo.html','app.js','styles.css','_headers'])await copyFile(file,'public/'+file);
