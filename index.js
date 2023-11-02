const createModel = require('./interface/createModel');
const createSchema = require('./interface/createSchema');
const Schema = require('./interface/Schema');
class DYNAMOManager {
  /**
   * Database Create Schema
   * @function createSchema
   * @param {Object} collectionStructure
   * @returns {Promise<Number>}
   */
  static createSchema = createSchema;

  /**
   * Database Create Model
   * @function createModel
   * @param {string} collectionName
   * @param {Object} newModel
   * @param {string} collectionNameInDb
   * @returns {Promise<Object>}
   */
  static createModel = createModel;
  static Schema = Schema;
}

module.exports = DYNAMOManager;