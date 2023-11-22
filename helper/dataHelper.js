const { ATTRIBUTE_TYPE, DEFAULT_VALUES } = require('../constants/constant');

module.exports.DataHelper = (data, schema) => {
    const items = {};

    for (const keyName in schema) {

        if (!keyName) {
            continue;
        }

        if (!schema[keyName] || !("AttributeType" in schema[keyName])) {
            console.log(`${keyName} Not found in schema`);
            continue;
        }

        // Adding default value if not found
        if (data[keyName] === undefined || data[keyName] === null) {
            if ("Default" in schema[keyName]) {
                data[keyName] = schema[keyName].Default;
            }
            else {
                data[keyName] = DEFAULT_VALUES[schema[keyName].AttributeType];
            }
        }

        // For Boolean convert the value to true | false.
        if (schema[keyName].AttributeType == ATTRIBUTE_TYPE.BOOL) {
            items[keyName] = {
                [schema[keyName].AttributeType]: !!data[keyName]
            }
        }

        else if (schema[keyName].AttributeType == ATTRIBUTE_TYPE.NUMBER) {
            items[keyName] = {
                [schema[keyName].AttributeType]: '' + data[keyName]
            }
        }

        else if (schema[keyName].AttributeType == ATTRIBUTE_TYPE.BINARY) {
            items[keyName] = {
                [schema[keyName].AttributeType]: data[keyName]
            };
        }

        else if (schema[keyName].AttributeType == ATTRIBUTE_TYPE.STRING) {
            items[keyName] = {
                [schema[keyName].AttributeType]: `${data[keyName]}`
            };
        }

        else if (schema[keyName].AttributeType == ATTRIBUTE_TYPE.LIST) {
            const arrData = this.ListHelper(keyName[data])

            if (arrData.length == 0) {
                continue;
            }

            items[keyName] = {
                [ATTRIBUTE_TYPE.LIST]: arrData
            }
        }

        else if (schema[keyName].AttributeType == ATTRIBUTE_TYPE.MAP) {

            // If schema is not present then throw error.
            if (!schema[keyName].Schema) {
                console.log("*** Schema is required ***");
                throw new Error("Schema is required for Map attribute Type");
            }
            const mapData = this.DataHelper(data[keyName], schema[keyName].Schema);

            if (Object.keys(mapData).length == 0) {
                continue;
            }

            // this.MapHelper(data[keyName], schema[keyName].Schema);
            items[keyName] = {
                [ATTRIBUTE_TYPE.MAP]: mapData
            }
        }

        else if (
            schema[keyName].AttributeType == ATTRIBUTE_TYPE.STRING_SET ||
            schema[keyName].AttributeType == ATTRIBUTE_TYPE.BINARY_SET ||
            schema[keyName].AttributeType == ATTRIBUTE_TYPE.NUMBER_SET
        ) {
            let valueData = [];
            if ( !data[keyName] || data[keyName].length === 0 ) {
                continue;
            }

            for (const val of data[keyName]) {
                valueData.push({
                    [schema[keyName].AttributeType[0]]: `${val}`
                });
            }
            items[keyName] ={ [schema[keyName].AttributeType]: valueData};
        }
    }

    return JSON.parse(JSON.stringify(items));
}

module.exports.ListHelper = (arrData) => {
    const data = [];

    if (!arrData || arrData.length === 0) {
        return [];
    }

    for (const val of arrData) {
        if (typeof val == 'string') {
            data.push({
                [ATTRIBUTE_TYPE.STRING]: val
            });
        } else if (typeof val == 'number') {
            data.push({
                [ATTRIBUTE_TYPE.NUMBER]: '' + val
            });
        }
        else if (val == null || val == undefined) {
            data.push({
                [ATTRIBUTE_TYPE.NULL]: true
            });
        }
        else if (Array.isArray(val)) {
            const arrData = this.ListHelper(val);
            data.push({
                [ATTRIBUTE_TYPE.LIST]: arrData
            });
        }
        else if (typeof val == 'object') {
            const mapData = this.MapHelper(val);
            data.push({
                [ATTRIBUTE_TYPE.MAP]: mapData
            });
        }
    }

    return data;
}

module.exports.MapHelper = (data, schema) => {

    const mapData = {};

    for (const subDocKey in data) {

        if (
            schema[subDocKey]?.AttributeType === ATTRIBUTE_TYPE.NUMBER ||
            (
                !schema[subDocKey] &&
                typeof data[subDocKey] == 'number'
            )
        ) {
            mapData[subDocKey] = {
                [ATTRIBUTE_TYPE.NUMBER]: '' + data[subDocKey]
            }
        }

        else if (
            schema[subDocKey]?.AttributeType === ATTRIBUTE_TYPE.STRING ||
            (
                !schema[subDocKey] &&
                typeof data[subDocKey] == 'string'
            )
        ) {
            mapData[subDocKey] = {
                [ATTRIBUTE_TYPE.STRING]: data[subDocKey]
            }
        }

        else if (
            schema[subDocKey]?.AttributeType === ATTRIBUTE_TYPE.BOOL ||
            (
                !schema[subDocKey] &&
                typeof data[subDocKey] == 'boolean'
            )
        ) {
            mapData[subDocKey] = {
                [ATTRIBUTE_TYPE.BOOL]: (!!data[subDocKey])
            }
        }

        else if (data[subDocKey] == null) {
            mapData[subDocKey] = {
                [ATTRIBUTE_TYPE.NULL]: true
            }
        }

        else if (
            schema[subDocKey]?.AttributeType === ATTRIBUTE_TYPE.LIST ||
            schema[subDocKey]?.AttributeType === ATTRIBUTE_TYPE.NUMBER_SET ||
            schema[subDocKey]?.AttributeType === ATTRIBUTE_TYPE.STRING_SET ||
            (
                !schema[subDocKey] &&
                Array.isArray(data)
            )
        ) {
            const arrData = this.ListHelper(data);
            mapData[subDocKey] = {
                [ATTRIBUTE_TYPE.LIST]: arrData
            }
        }

        else if (
            schema[subDocKey]?.AttributeType === ATTRIBUTE_TYPE.MAP ||
            (
                !schema[subDocKey] &&
                typeof data[subDocKey] == 'object'
            )
        ) {
            const subMapData = this.MapHelper(data[subDocKey], schema[subDocKey]?.Schema);
            mapData[subDocKey] = {
                [ATTRIBUTE_TYPE.MAP]: subMapData
            }
        }
    }

    return mapData;
}

module.exports.ExtractDataTypeFromArray = (data) => {
    const items = [];
    for (const value of data) {
        const item = this.ExtractDataType(value);
        items.push(item);
    }
    return items;
}

module.exports.ExtractDataType = (data) => {
    let items = {};

    if (Array.isArray(data)) {
        items = this.ExtractDataTypeFromArray(data);
    }
    else if (typeof data === 'object') {
        items =  {};
        for (const key in data) {
            items[key] = Object.values(data[key])[0]
        }
    }

    return items;
}