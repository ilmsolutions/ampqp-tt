import * as ampqp from 'amqplib/callback_api';

export class Subscriber {
    //private channel;
    private connection : ampqp.Connection | null = null;
    private channel : ampqp.Channel | null = null;

    constructor(){
       this.connection = null;
    }

    init(connection: ampqp.Connection){
       this.connection = connection;
       return this;
    }

    private assertChannel() : Promise<any>{
       let self = this;

       return new Promise((resolve, reject) => {
        if(self.channel) resolve(self.channel);
        else if(self.connection) {
          self.connection.createChannel(function(err, ch) {
            if (err) reject(err);
            ch.on("error", function(err) {
              console.error("[AMQP] channel error", err.message);
            });
        
            ch.on("close", function() {
              console.log("[AMQP] channel closed");
              self.channel = null;
            });
  
            self.channel = ch;
  
            resolve(self.channel);
  
          });
        }
        else
          throw new Error('Connection closed');
      })       
    }

    public start() : Promise<any>{
      let self = this 
      ;
      return self.assertChannel()
                 .then(ch => {
                   //self.channel = ch;
                   return self;
                 })
                 .catch(err => {
                  self.connection && self.connection.close();
                 })
      ; 
    }
  
    subscribeTopics(exchange: string, queue: string, topics: string[],
                    handler: (msg: ampqp.Message | null, callback?:(ok: ampqp.Replies.Consume) => void) => void){
      let self = this 
      ,   processMsg = this.processMsg(handler);

      return self.start()
          .then(() => { 
            self.channel?.assertExchange(exchange, 'topic')

            return new Promise((resolve, reject) => { 
              self.channel?.assertQueue(queue, {durable: true}, function(err, ok){
                  if(err) reject(err);
                  if(!self.channel) return;

                  let channel = self.channel; 

                  topics.map(topic => {
                    channel.bindQueue(ok.queue, exchange, topic);
                  });
                  
                  channel.consume(ok.queue, processMsg, {}, function(cerr, cok){
                     if(cerr) reject(cerr);
                 
                     resolve(cok.consumerTag);
                     
                  }); 
              });
            })
        ;

      })
    }

    unsubscribeConsumer(ctag: string){
      let self = this
      ;

      self.start()
          .then(() => {
          return new Promise((resolve, reject) => {            
            self.channel?.cancel(ctag, function(err, ok) {
              if(err) console.log('Error while cancelling consumer:', err);
              // console.log('Consumer cancelled successfully ', ok);
            });

            resolve(true);

          });

      })   
    }

    assertSubscribeQueue(queue: string){
        let self = this;
        return self.start()
            .then(() => { 
                 self.channel?.assertQueue(queue, {durable: true}, function(err, ok){
                      if (err) throw new Error(err.message);
                      console.log(`${queue} created.`);
                      return ok;
                 });
        });
    }

    subscribe(queue: string,
        handler: (msg: ampqp.Message | null, callback?:(ok: ampqp.Replies.Consume) => void) => void){
      let self = this
      ,   processMsg = this.processMsg(handler);

      self.start()
          .then(() => {
            
            self.channel?.prefetch(10);
            self.channel?.assertQueue(queue, { durable: true }, function(err, _ok) {
              if (err) throw new Error(err.message);
              self.channel?.consume(queue, processMsg, { noAck: false });
              console.log("Worker is started");
            });
      });

    }

    processMsg(handler: (msg: ampqp.Message | null, callback?:(ok: ampqp.Replies.Consume) => void) => void){
      let self = this;
      
    
      return (msg : ampqp.Message | null) => {
          if(!self.channel || !msg) return;
      
          handler(msg, function(ok: ampqp.Replies.Consume){
            try {              
              if (ok)
                self.channel?.ack(msg);
              else 
                self.channel?.reject(msg, true);
            } 
            catch(err){
              ErrorHandler(err);
            } 
          })
      };
   } 
}

const ErrorHandler = (err: any) => {
    if(err instanceof Error){
           throw new Error(err.message);
    }
}