var dgram  = require('dgram')
  , sys    = require('sys')
  , net    = require('net')
  , config = require('./config')

var counters = {};
var timers = {};
var debugInt, flushInt, server;

function measureForKey (key, fields){
  var sampleRate = 1;
  var finalKey = key;
  if (fields[1] === undefined) {
    sys.log('Bad line: ' + fields);
  } else {
    if (fields[1].trim() == "ms") {
      if (!(fields[3] === undefined)) {
        finalKey = fields[3] + '.timers.' + key ;
      } else {
        finalKey = 'other.unknownHosts.timers.' + key ;
      }
      if (! timers[finalKey]) {
        timers[finalKey] = [];
      }
      timers[finalKey].push(Number(fields[0] || 0));
    } else {
      if (!(fields[3] === undefined)) {
        finalKey = fields[3] + "." + key ;
      } else {
        finalKey = 'other.unknownHosts.' + key ;
      }
      if (fields[2] && fields[2].match(/^@([\d\.]+)/)) {
        sampleRate = Number(fields[2].match(/^@([\d\.]+)/)[1]);
      }
      if (! counters[finalKey]) {
        counters[finalKey] = 0;
      }
      counters[finalKey] += Number(fields[0] || 1) * (1 / sampleRate);
    }
  }
}


config.configFile(process.argv[2], function (config, oldConfig) {
  if (! config.debug && debugInt) {
    clearInterval(debugInt); 
    debugInt = false;
  }

  if (config.debug) {
    if (debugInt !== undefined) { clearInterval(debugInt); }
    debugInt = setInterval(function () { 
      sys.log("Counters:\n" + sys.inspect(counters) + "\nTimers:\n" + sys.inspect(timers));
    }, config.debugInterval || 10000);
  }

  if (server === undefined) {
    server = dgram.createSocket('udp4', function (msg, rinfo) {
      if (config.dumpMessages) { sys.log(msg.toString()); }
      var bits = msg.toString().split(':');
      var key = bits.shift()
                    .replace(/\s+/g, '_')
                    .replace(/\//g, '-')
                    .replace(/[^a-zA-Z_\-0-9\.]/g, '');

      if (bits.length == 0) {
        bits.push("1");
      }

      for (var i = 0; i < bits.length; i++) {
        var fields = bits[i].split("|");
        measureForKey(key, fields);
      }
    });

    server.bind(config.port || 8125);

    var flushInterval = Number(config.flushInterval || 10000);

    flushInt = setInterval(function () {
      var statString = '';
      var ts = Math.round(new Date().getTime() / 1000);
      var numTimers = 0;
      var numCounters = 0;
      var numTimersWithValues = 0;
      var key;

      for (key in counters) {
        var value = counters[key] / (flushInterval / 1000);
        var message = key + ' ' + value + ' ' + ts + "\n";
        statString += message;
        counters[key] = 0;
	numCounters += 1;
      }

      for (key in timers) {
        numTimers += 1;
        if (timers[key].length > 0) {
          var pctThreshold = config.percentThreshold || 90;
          var values = timers[key].sort(function (a,b) { return a-b; });
          var count = values.length;
          var min = values[0];
          var max = values[count - 1];

          var mean = min;
          var maxAtThreshold = max;

          if (count > 1) {
            var thresholdIndex = Math.round(((100 - pctThreshold) / 100) * count);
            var numInThreshold = count - thresholdIndex;
            values = values.slice(0, numInThreshold);
            maxAtThreshold = values[numInThreshold - 1];

            // average the remaining timings
            var sum = 0;
            for (var i = 0; i < numInThreshold; i++) {
              sum += values[i];
            }

            mean = sum / numInThreshold;
          }

          timers[key] = [];

          var message = "";
          message += key + '.mean ' + mean + ' ' + ts + "\n";
          message += key + '.upper ' + max + ' ' + ts + "\n";
          message += key + '.upper_' + pctThreshold + ' ' + maxAtThreshold + ' ' + ts + "\n";
          message += key + '.lower ' + min + ' ' + ts + "\n";
          message += key + '.count ' + count + ' ' + ts + "\n";
          statString += message;
          numTimersWithValues +=1;  
        }
      }

      statString += 'statsd.numCounters ' + numCounters + ' ' + ts + "\n";
      statString += 'statsd.numTimers ' + numTimers + ' ' + ts + "\n";
      statString += 'statsd.numTimersWithValues ' + numTimersWithValues + ' ' + ts + "\n";
      
      try {
        var graphite = net.createConnection(config.graphitePort, config.graphiteHost);
        graphite.addListener('error', function(connectionException){
          if (config.debug) {
            sys.log(connectionException);
          }
        });
        graphite.on('connect', function() {
          this.write(statString);
          this.end();
        });
      } catch(e){
        if (config.debug) {
          sys.log(e);
        }
      }

    }, flushInterval);
  }

});

