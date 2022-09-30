import { dirname, join } from "path";
import { promisify } from "util";
import { readdir, createReadStream, createWriteStream, statSync, ReadStream } from "fs";
import { pipeline, Transform } from "stream";
import { randomUUID } from "crypto";

import csvtojson from "csvtojson";
import StreamConcat from "stream-concat";

const pipelineAsync = promisify(pipeline);
const readDirAsync = promisify(readdir);


const { pathname: currentFile } = new URL(import.meta.url);
const currentWorkDir = dirname(currentFile);

const datasetWorkDir = `${currentWorkDir}/data`.replace("C:/", "");
const outputDataFile = `${currentWorkDir}/finalFile.csv`.replace("C:/", "");

const informationFilesToReading = [];
const filesToReading = (await readDirAsync(datasetWorkDir)).filter(
  (file) => !!~file.indexOf(".csv")
);

let timeInProcessingDataInSegs = 0;
let currentSizeFinalFile = 0;

setInterval(() => {
  timeInProcessingDataInSegs++;

  const messages = [
    `============================================================`,
    `==> Arquivos : ${filesToReading}`,
    `==> Processando ${ByteToMB_Rounded(
      currentSizeFinalFile
    )}MB : ${progressBar(timeInProcessingDataInSegs)}`,
    `============================================================`,
  ];

  applyInterface(messages);
}, 1000).unref();

const streams = filesToReading.map((document) => {
  const pathFile = join(datasetWorkDir, document);
  const infoFile = statSync(pathFile);
  informationFilesToReading.push({
    path: join(datasetWorkDir, document),
    size: infoFile.size,
  });
  return createReadStream(pathFile);
});

const mergedStreams = new StreamConcat(streams);
const createOutFile = createWriteStream(outputDataFile).on("finish", () => {
  const messages = [
    `=============== PROCESSO FINALIZADO ========================`,
    `==> Arquivos : ${filesToReading}`,
    `==> Processado: ${ByteToMB_Rounded(currentSizeFinalFile)} MB `,
    `==> Duração : ${timeInProcessingDataInSegs}s`,
    `==> Localização : ${outputDataFile}`,
    `============================================================`,
  ];
  applyInterface(messages);
});
const handlerFile = new Transform({
  transform: (chunk, enc, cb) => {
    let item = JSON.parse(chunk);
    item = JSON.stringify({
      id: randomUUID(),
      ...item,
    });
        
    currentSizeFinalFile += Buffer.byteLength(item);
    return cb(null, item);
  },
});

pipelineAsync(mergedStreams, csvtojson(), handlerFile, createOutFile).catch(
  (e) => console.log("DEU RUIM ! " + e.message)
);

function progressBar(progress) {
  let bar = "";
  for (let ps = 0; ps <= progress; ps++) {
    bar += ".";
  }
  return bar;
}
function ByteToMB_Rounded(byte) {
  return Math.round(byte / 1000000);
}
function applyInterface(messages) {
  console.clear();
  for (const message of messages) {
    process.stdout.write(message + "\n");
  }
}
