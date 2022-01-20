import * as ampqp from 'amqplib/callback_api';
import {Publisher} from './internals/publisher';
import {Subscriber} from './internals/subscriber';

interface params {
    url: string; 
}

export const AMPQP = (config: params) => {
    const {url} = config;
    let ampqpConn: ampqp.Connection | null = null;
    const Pub = new Publisher();
    const Sub = new Subscriber();
    const start = () => {
      return new Promise((resolve, reject) => {
        ampqp.connect(url + "?heartbeat=60", function(err, conn) {
          if (err) {
            console.error("[AMQP]", err.message);
            return setTimeout(start, 1000);
          }
          conn.on("error", function(err) {
            if (err.message !== "Connection closing") {
              console.error("[AMQP] conn error", err.message);
            }
          });
          conn.on("close", function() {
            console.error("[AMQP] reconnecting");
            return setTimeout(start, 1000);
          });
          console.log("[AMQP] connected");
          ampqpConn = conn;
  
          resolve(true);
         
        });
      }).then(status => {
        if(!status || !ampqpConn) 
           throw new Error('[AMQP] Failed to establish connection.');

        return Promise.all([Pub.init(ampqpConn).start(), Sub.init(ampqpConn).start()])
           .then(res => {
              return {
                publisher: res[0]
              , subscriber: res[1]
              }
           })
           ; 
      }).catch(err => {
        console.error("[AMQP] error", err); 
        ampqpConn?.close();
        return {publisher: null, subscriber: null};
      }) 
      ; 
    } 
  
   return start();
  
  }
  