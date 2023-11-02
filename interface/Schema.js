const { BILLING_MODE, KEY_TYPE } = require('../constants/constant');
const AWS = require('aws-sdk');

const Schema = module.exports = function (name, schema, billingMode = BILLING_MODE.PAY_REQUEST, provisionedThroughput = { readCapacity: 0, writeCapacity: 0 }) {

    this.schemaName = name;

    // Verify schema is pending.
    this.schema = schema;

    if (billingMode !== BILLING_MODE.PAY_REQUEST && billingMode !== BILLING_MODE.PROVISIONED) {
        throw new Error(`Billing mode must be ${Object.values(BILLING_MODE)}`)
    }
    this.billingMode = billingMode;

    if (billingMode === BILLING_MODE.PROVISIONED) {

        /* For provisioned throughput required read (RCU) and write capacity units (WCU)   */
        if (
            !("readCapacity" in provisionedThroughput) ||
            provisionedThroughput.readCapacity === 0 ||
            !("writeCapacity" in provisionedThroughput) ||
            provisionedThroughput.writeCapacity === 0
        ) {
            throw new Error("REQUIRED READ OR WRITE Capacity for billing mode " + billingMode);
        }

        this.provisionedThroughput = provisionedThroughput;
    }
    else if (billingMode === BILLING_MODE.PAY_REQUEST) {
        this.provisionedThroughput = {};
    }
}

Schema.prototype.CreateTable = async () => {
    const DynamoDB = new AWS.DynamoDB();
    const KeySchema = [], AttributeDefinitions = [];

    for (const key in this.schema) {

        /* Check if KeyType is present then add HASH|RANGE (Primary Key | sort Key) */
        if (
            "KeyType" in this.schema[key] &&
            (
                this.schema[key].KeyType === KEY_TYPE.PRIMARY_KEY ||
                this.schema[key].KeyType === KEY_TYPE.RANGE
            )
        ) {
            KeySchema.push({
                AttributeName: key,
                KeyType: this.schema[key].keyType
            });
        }

        AttributeDefinitions.push({
            AttributeName: key,
            AttributeType: this.schema[key].AttributeType
        });
    }

    DynamoDB.createTable({
        AttributeDefinitions,
        KeySchema,
        BillingMode: this.billingMode,
        ProvisionedThroughput: this.provisionedThroughput,
        TableName: this.schemaName
    }, (err, data) => {
        if (err) {
            console.log("Error Creating the table", err);
            throw new Error(err);
        }
        console.log("Created Data" + data);
    });

    let status = "CREATING";

    while (status === "CREATING") {
        const data = await this.DescribeTable();
        if (data?.TableStatus === "ACTIVE") {
            status = "ACTIVE"
        }
        await setTimeout(() => { }, 1000);
    }
}

Schema.prototype.DescribeTable = async () => {
    const DynamoDB = new AWS.DynamoDB();
    DynamoDB.describeTable(this.schemaName, (err, data) => {
        if (err) {
            console.log("Error on describeTable", err);
        }
        else {
            return data;
        }
    });
}