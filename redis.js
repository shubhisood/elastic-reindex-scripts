const bluebird = require('bluebird');
const Redis = require('ioredis');

Redis.Promise = bluebird;

const redisObj = {
  get: () => {
   console.log('Creating new redis instance...');
    let rclient;
    const commonSettings = {
      showFriendlyErrorStack: false,
      port: 6379,
      host: 'localhost',
      db: '0',

      reconnectOnError: (err) => {
        const targetError = 'READONLY';
        if (err.message.slice(0, targetError.length) === targetError) {
          // Only reconnect when the error starts with "READONLY"
          return true; // or `return 1;`
        }
        return false;
      }
    };
    rclient = new Redis({ ...commonSettings });
    rclient.instance_status = true;
    rclient.on('error', (err) => {
      const errorList = ['ECONNREFUSED', 'READONLY'];
      if (new RegExp(errorList.join("|")).test(err.message)) {
        rclient.instance_status = false;
        console.log(err);
      }
      return err;
    });
    rclient.on('connect', () => {
      rclient.instance_status = true;
    });

    rclient.checkStatus = () => rclient.instance_status;
    
    console.log('created new instance')

    return rclient;
  },
};
var rclient = redisObj.get();

module.exports = rclient;
