// Dependencies.
const express = require('express');
const bodyParser = require('body-parser');
const readline = require('readline');
const ipaddr = require('ipaddr.js');
const { exec } = require('child_process');
const path = require('path');
const fs = require('fs');

// Configurations.
const PORT = 8000;

// Constants.
const STATUS_MACHINE_READY = -1;
const STATUS_MACHINE_WORKING = -2;
const STATUS_MACHINE_CLEANUP = -3;
const STATUS_EXPERIMENT_WAITING = -4;
const STATUS_EXPERIMENT_WORKING = -5;
const STATUS_EXPERIMENT_DONE = -6;

function statusMessage(status) {
  switch (status) {
    case STATUS_MACHINE_READY: return "ready";
    case STATUS_MACHINE_WORKING: return "working";
    case STATUS_MACHINE_CLEANUP: return "cleaning up";
    case STATUS_EXPERIMENT_WAITING: return "waiting";
    case STATUS_EXPERIMENT_WORKING: return "working";
    case STATUS_EXPERIMENT_DONE: return "done";
    default: return "UNKNOWN " + status;
  }
}

// Create express webapp and stdin readers.
const app = express();
app.use(bodyParser.text({ inflate: true, limit: '5mb', type: 'text/plain' }));

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
  terminal: false
});

// store the time it took to finish things.
// machine/client id ==> time it took for that machine to finish
let clientTimes;
let partyTimes;

/*** Helpers ***/
function formatIP(ipString) {
  if (ipaddr.IPv4.isValid(ipString)) {
    return ipString;
  } else if (ipaddr.IPv6.isValid(ipString)) {
    var ip = ipaddr.IPv6.parse(ipString);
    if (ip.isIPv4MappedAddress()) {
      return ip.toIPv4Address().toString();
    } else {
      // ipString is IPv6
      throw 'Cannot handle ipv6 ' + ipString;
    }
  } else {
    throw 'Invalid IP! ' + ipString;
  }
}

/*** Global State and manipulation.
 *** Node is single threaded and we dont use asynchronous callbacks
 *** ==> We dont need synchronization. */
// Machine tracking.
const machineIPs = {};
const allMachines = [];
// Experiment tracking.
const allExperiments = [];
const jobQueue = [];
const resultQueue = [];

// Machine class.
function Machine(ip) {
  this.ip = ip;
  this.id = allMachines.length;
  this.ready();
  allMachines.push(this);
}
Machine.prototype.ready = function () {
  if (this.status != STATUS_MACHINE_READY) {
    console.log("Machine", this.id, "is ready");
  }
  this.status = STATUS_MACHINE_READY;
  this.experiment = null;
  this.ping = Date.now();
};
Machine.prototype.assignJob = function () {
  this.ready();
  if (jobQueue.length == 0) {
    return false;
  } else {
    this.status = STATUS_MACHINE_WORKING;
    this.experiment = jobQueue.shift();
    this.experiment.assignMachine(this);
    console.log("Assigned experiment", this.experiment.id, "to machine", this.id);
    return this.experiment;
  }
};
Machine.prototype.finished = function (result) {
  console.log("Experiment", this.experiment.id, "on machine", this.id, "finished");
  this.ping = Date.now();
  this.status = STATUS_MACHINE_CLEANUP;
  this.experiment.finished(result);
  this.experiment = null;
};
Machine.prototype.lastPing = function () {
  return ((Date.now() - this.ping) / 1000.0) + "sec";
};
Machine.prototype.toString = function () {
  return "Machine " + this.id + ", status: " + statusMessage(this.status)
      + ", last ping: " + this.lastPing() + ", ip: " + this.ip;
};
Machine.findMachine = function (ip) {
  if (!machineIPs[ip]) {
    machineIPs[ip] = new Machine(ip);
  }
  return machineIPs[ip];
};

// Experiment class.
function Experiment(name, loadFile, type) {
  this.id = allExperiments.length;
  this.name_ = name;
  this.content = fs.readFileSync(loadFile, 'utf8');;
  this.status = STATUS_EXPERIMENT_WAITING;
  this.startTime = null;
  this.machine = null;
  this.result = null;
  this.type = type;
  allExperiments.push(this);
  jobQueue.push(this);
}
Experiment.prototype.timeElapsed = function () {
  switch (this.status) {
    case STATUS_EXPERIMENT_WORKING:
      return "working for " + ((Date.now() - this.startTime) / 1000.0) + "sec";
    default:
      return statusMessage(this.status);
  }
};
Experiment.prototype.assignMachine = function (machine) {
  this.status = STATUS_EXPERIMENT_WORKING;
  this.startTime = Date.now();
  this.machine = machine;
};
Experiment.prototype.finished = function (result) {
  this.status = STATUS_EXPERIMENT_DONE;
  this.result = result;
  resultQueue.push(this);
  fs.writeFileSync("orchestrator/server/result" + this.id, this.result);
};
Experiment.prototype.toString = function () {
  let str = this.type + " Experiment " + this.id + " (" + this.name_ + ")";
  str += ", status: " + this.timeElapsed();
  if (this.machine != null) {
    str += ", machine: " + this.machine.id;
  }
  return str;
};

/*** Routes for worker machines ***/
app.get('/ready', (req, res) => {
  const ip = formatIP(req.connection.remoteAddress);
  const machine = Machine.findMachine(ip);
  const experiment = machine.assignJob();
  if (experiment) {
    res.type("text/plain");
    res.send("# " + experiment.type + "\n" + experiment.content);
  } else {
    res.type("text/plain");
    res.send("");
  }
});

app.post('/done', (req, res) => {
  const ip = formatIP(req.connection.remoteAddress);
  const machine = Machine.findMachine(ip);
  machine.finished(req.body);
  res.send("");
});

/*** CLI interfae ***/
app.listen(PORT, () => {
  console.log(`Orchastrator listening at http://localhost:${PORT}`);
  // Start the command line interface.
  // Handle commands!
  rl.on('line', function (line) {
    line = line.trim();
    if (line.startsWith('help')) {
      console.log('Available commands:');
      console.log('- exit');
      console.log('- machines');
      console.log('- experiments');
      console.log('- result <experiment id>');
      console.log('- gdprbench <tag> <path/to/load/file>');
      console.log('- load <path/to/dir/containing/loads> [baseline|pelton]');
    }
    if (line.startsWith('exit')) {
      process.exit(0);
      return;
    }
    if (line.startsWith('machines')) {
      for (const machine of allMachines) {
        console.log(machine.toString());
      }
    }
    if (line.startsWith('experiments')) {
      for (const experiment of allExperiments) {
        console.log(experiment.toString());
      }
    }
    if (line.startsWith('result')) {
      const id = parseInt(line.split(" ")[1]);
      const experiment = allExperiments[id];
      if (experiment) {
        console.log(experiment.toString());
        if (experiment.status == STATUS_EXPERIMENT_DONE) {
          console.log("");
          console.log("");
          console.log("");
          console.log("Result ===============");
          console.log(experiment.result);
          console.log("======================");
          console.log("");
          console.log("");
          console.log("");
        }
      } else {
        console.log("Experiment not found");
      }
    }
    if (line.startsWith('gdprbench')) {
      const split = line.split(" ");
      const name = split[1];
      const loadFile = split[2];
      const experiment1 = new Experiment(name, loadFile, "Baseline");
      const experiment2 = new Experiment(name, loadFile, "Pelton");
      console.log(experiment1.toString());
      console.log(experiment2.toString());
    }
    if (line.startsWith('load')) {
      const split = line.split(" ");
      const directory = split[1];
      const type = split[2];
      const sep = directory.endsWith("/") ? "" : "/";
      for (const file of fs.readdirSync(directory)) {
        const path = directory + sep + file;
        if (fs.lstatSync(path).isFile()) {
          if (type.toLowerCase() == "baseline")  {
            const experiment = new Experiment(file, path, "Baseline");
            console.log(experiment.toString());
          } else {
            const experiment = new Experiment(file, path, "Pelton");
            console.log(experiment.toString());
          }
        }
      }
    }

    console.log("> ");
  });
});

