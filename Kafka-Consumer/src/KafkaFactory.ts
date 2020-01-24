
import { KafkaClient } from "kafka-node";

class KafkaFactory {

    private kafka = require("kafka-node");

    private kafkaClient = null;

    /**
     * Create oject of KafkaFactory
     * @param host kafka host
     * @param port kafka port number
     */
    public constructor(host: string, port: number) {
        this.kafkaClient = new KafkaClient({ kafkaHost: host + ":" + port });
    }

    public getKafkaConsumer(topicNames: string[]) {
        const topicArray = new Array();
        for (const name of topicNames) {
            topicArray.push({ topic: name, partition: 0 });
        }
        // console.log('topicArray', topicArray);
        const kafkaConsumer = new this.kafka.Consumer(this.kafkaClient, topicArray);

        return kafkaConsumer;

    }

    public getKafkaProducer() {
        const kafkaProducer = new this.kafka.Producer(this.kafkaClient);
        // console.log("kafkaProducer:", kafkaProducer);
        return kafkaProducer;
    }

    public getKafkaAdmin() {
        const kafkaAdmin = new this.kafka.Admin(this.kafkaClient);
        return kafkaAdmin;
    }

}

export default KafkaFactory;
