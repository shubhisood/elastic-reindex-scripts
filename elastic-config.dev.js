var elastic = require("elasticsearch");
var Bluebird = require('bluebird');

// config @ https://www.elastic.co/guide/en/elasticsearch/client/javascript-api/current/configuration.html

var host = {
  host: 'https://search-elastic-dev-cluster-5ogyygfvgft6bg2g6h2wafsaje.us-west-1.es.amazonaws.com',
  defer: function () {
    return Bluebird.defer();
  }
};
var elasticClient = new elastic.Client(host);
module.exports = elasticClient;
