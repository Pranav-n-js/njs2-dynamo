const constants = {};

constants.BILLING_MODE = {
    PAY_REQUEST: 'PAY_PER_REQUEST',
    PROVISIONED: 'PROVISIONED'
};

constants.KEY_TYPE = {
    'PRIMARY_KEY': 'HASH',
    'RANGE': 'RANGE'
};

constants.ATTRIBUTE_TYPE = {
    STRING: 'S',
    NUMBER: 'N',
    BINARY: 'B',
    BOOL: 'BOOL',
    NULL: 'NULL',
    MAP: 'M',
    LIST: 'L',
    STRING_SET: 'SS',
    NUMBER_SET: 'NS',
    BINARY_SET: 'BS'
};

constants.DEFAULT_VALUES = {
    S: '',
    N: 0,
    B: null,
    BOOL: false,
    NULL: null,
    MAP: {},
    LIST: [],
    SS: [],
    NS: [],
    BS: [],
};

constants.UPDATE_TYPE = {
    SET: 1,
    ADD: 2,
    REMOVE: 3,
};

module.exports = constants;