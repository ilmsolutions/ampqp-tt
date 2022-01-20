import * as ampqp from 'amqplib/callback_api';
 
 
interface PubQueueItem {
  exchange: string;
  routingKey: string;
  content: string;
}
var offlinePubQueue : Array<PubQueueItem> = new Array<PubQueueItem>();

export class Publisher {
    private connection : ampqp.Connection | null = null;
    private channel : ampqp.Channel | null = null;
  
    constructor(){
       
    }
  
    init(connection: ampqp.Connection){
       this.connection = connection;
       return this;
    }
  
    private assertChannel() : Promise<ampqp.Channel>{
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
  
  
     public start() : Promise<Publisher> {     
       let self = this;
       return self.assertChannel()
                  .then(ch => {
                    //self.channel = ch;
  
                    while (offlinePubQueue.length > 0) {
                      let {exchange, routingKey, content} = offlinePubQueue.shift() || {};      
                      if(exchange && routingKey && content)      
                         self.publish(exchange, routingKey, content);
                    }   
  
                    return self;
                  })
       ;  
     }
  
     public publishTopic(exchange: string, routingKey: string, content: string){
       const self = this;

       self.start()
       .then(() => { 
        self.channel?.assertExchange(exchange, 'topic', {durable: true});
        self.publish(exchange, routingKey, content);
       })
       .catch((e: unknown) => {
          console.error("[AMQP] publish", e); 
          offlinePubQueue.push({exchange, routingKey, content});
       })
       ;
     }
  
     public publish(exchange: string, routingKey: string, content: string, callback?: (err: any, ok?: any) => void){
       const self = this;
    
       self.start()
       .then(() => {
         if(!self.channel?.publish(exchange, routingKey, Buffer.from(content), {persistent: true})){
           
             throw  new Error('[AMQP] Publish failed')               
         } 

         callback && callback(null, true); 
       })
       .catch((e: unknown) => {
        console.error("[AMQP] publish", e); 
        offlinePubQueue.push({exchange, routingKey, content});
        self.connection?.close(); 
       })
       ; 
     }  
  }
  

