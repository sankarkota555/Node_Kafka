import Config from "./Config";

class MongoFactory {

    public mongoClient: any;

    public async openMongoConnection(maxPoolSize) {
        return new Promise((resolve, reject) => {
            const MongoClient = require("mongodb").MongoClient;
            const env = process.env.ENV.trim();
            switch (env) {
                case "prod":
                    const prodClient = new MongoClient(Config.mongoUrl,
                        {
                            poolSize: maxPoolSize,
                            useNewUrlParser: true,
                            useUnifiedTopology: true,
                        });
                    prodClient.connect((connErr) => {
                        if (connErr) {
                            console.log("Mongo connection error:", connErr);
                        }
                        this.mongoClient = prodClient.db(Config.mongoDatabaseName);
                        resolve(this.mongoClient);
                    });
                    break;
                case "dev":
                default:
                    MongoClient.connect(Config.mongoUrl,
                        { poolSize: maxPoolSize, auto_reconnect: true, useUnifiedTopology: true },
                        (error, client) => {
                            if (error) {
                                console.error("Error during connection to mongo server", error);
                                throw new Error(error);
                            }
                            this.mongoClient = client.db(Config.mongoDatabaseName);
                            resolve(this.mongoClient);
                        });
            }

        });
    }
}

export default MongoFactory;
