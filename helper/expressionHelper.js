module.exports.generateUpdateExpression = (updateValue, updateKey, initialKey = null) => {

    let updateExpression = 'SET ';
    let value = {}, key = {};

    if (updateValue.length == updateKey.length) {
        updateKey.forEach((upKey, index) => {
            let currKey = ""

            if (initialKey) currKey = initialKey + `#k${upKey}${index}`;
            else currKey = `#k${upKey}${index}`;

            updateExpression += ` ${currKey} = :v${upKey}${index}`;

            if (updateKey[index + 1]) {
                updateExpression += ', '
            }
            value[`:v${upKey}${index}`] = updateValue[index];
            key[currKey] = upKey;
        })
    }
    return {
        UpdateExpression: updateExpression,
        ExpressionAttributeNames: key,
        ExpressionAttributeValues: value
    }
}
