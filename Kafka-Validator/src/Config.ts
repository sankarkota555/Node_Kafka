class Config {

    public mongoUrl: string;
    public mongoDatabaseName: string;

    public publishedCollectionName: string = "kafka_data_published";

    public receivedCollectionName: string = "kafka_data";

    constructor() {
        let mongoHost: string;
        let mongoPort: number;
        const env = process.env.ENV.trim();
        console.log("Running with environment:", env);
        switch (env) {
            case "prod":
                this.mongoDatabaseName = "Node_Test";
                this.mongoUrl = "mongodb+srv://iotdata:admin123" +
                    "@cluster0-v9hbe.mongodb.net/test?retryWrites=true&w=majority";
                break;
            case "dev":
            default:
                mongoHost = "127.0.0.1";
                mongoPort = 25015;
                this.mongoDatabaseName = "Temp";
                this.mongoUrl = "mongodb://" + mongoHost + ":" + mongoPort + "/" + this.mongoDatabaseName + "?retryWrites=true&w=majority";
        }
    }

}

export default new Config();
