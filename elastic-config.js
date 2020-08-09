var elastic = require("elasticsearch");
var Bluebird = require('bluebird');

// config @ https://www.elastic.co/guide/en/elasticsearch/client/javascript-api/current/configuration.html

var host = {
  host: 'http://localhost:9200',
  defer: function () {
    return Bluebird.defer();
  }
};
var elasticClient = new elastic.Client(host);
module.exports = elasticClient;
