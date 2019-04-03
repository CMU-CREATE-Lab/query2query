const JSendClientValidationError = require('jsend-utils').JSendClientValidationError;

const MIN_OFFSET = 0;
const MAX_OFFSET = Number.MAX_VALUE;
const MIN_LIMIT = 1;
const DEFAULT_LIMIT = 20;

const TOKEN_SEPARATOR = ',';
const WHERE_JOIN_AND = 'AND';
const WHERE_JOIN_OR = 'OR';
const DEFAULT_WHERE_JOIN = WHERE_JOIN_AND;
const VALID_WHERE_JOINS = [WHERE_JOIN_AND, WHERE_JOIN_OR];

// allowed operators are <>, <=, >=, <, >, and =
const WHERE_OPERERATORS_REGEX = /(<>|<=|>=|<|>|=)/;

const NULL_VALUE = "__NULL__";

const DEFAULT_TOKEN_PROCESSOR = function(token) {
   return {
      field : token,
      expression : token
   };
};

// heavily based on string's implementation...no need to depend on the string module for just this one thing
const toBoolean = function(valStr) {
   if (Object.prototype.toString.call(valStr) === "[object String]") {
      const s = valStr.toLowerCase();
      return s === 'true' || s === 'yes' || s === 'on' || s === '1';
   }
   else {
      return valStr === true || valStr === 1;
   }
};

function Query2Query() {

   const allowedSelectFieldsArray = [];
   const allowedSelectFields = {};
   const allowedWhereFields = {};
   const allowedOrderByFields = {};
   const allowNullValue = {};
   const dataTypes = {};

   this.addField = function(fieldName, allowWhere, allowOrderBy, canHaveNullValue, dataType) {
      if (typeof fieldName !== 'undefined' && fieldName != null) {
         if (!(fieldName in allowedSelectFields)) {
            allowedSelectFieldsArray.push(fieldName);
            allowedSelectFields[fieldName] = true;

            // record whether the field can have a null value
            allowNullValue[fieldName] = !!canHaveNullValue;

            // if the data type is defined, then see whether it's valid
            if (typeof dataType !== 'undefined' && dataType != null) {
               dataType = dataType.trim().toUpperCase();

               // check whether the requested type is valid
               if (dataType in Query2Query.types) {
                  // Don't bother recording the type for this field if it's a string--all the values will
                  // start out as strings since they come from the query string, so there's no conversion
                  // necessary
                  if (dataType !== Query2Query.types.STRING) {
                     dataTypes[fieldName] = dataType
                  }
               }
               else {
                  throw new TypeError("Invalid Query2Query field data type: " + dataType);
               }
            }
         }
         if (!!allowWhere) {
            allowedWhereFields[fieldName] = true;
         }
         if (!!allowOrderBy) {
            allowedOrderByFields[fieldName] = true;
         }
      }
   };

   this.parse = function(queryString, callback, maxLimit) {
      const self = this;
      process.nextTick(function() {
         try {
            callback(null, self.parseSync(queryString, maxLimit));
         }
         catch (e) {
            callback(e);
         }
      });
   };

   this.parseSync = function(queryString, maxLimit) {
      maxLimit = Math.max(MIN_LIMIT, (maxLimit || DEFAULT_LIMIT));

      const validationErrors = [];
      const fields = arrayify(queryString.fields);
      let whereAnd = arrayify(queryString.whereAnd, true);
      const whereOr = arrayify(queryString.whereOr, true);
      let whereJoin = arrayify(queryString.whereJoin || DEFAULT_WHERE_JOIN);
      const orderBy = arrayify(queryString.orderBy);
      const offset = parseIntAndEnforceBounds(queryString.offset, MIN_OFFSET, MIN_OFFSET, MAX_OFFSET);
      const limit = parseIntAndEnforceBounds(queryString.limit, maxLimit, MIN_LIMIT, maxLimit);

      // where is a synonym for whereAnd, so just concatenate them
      whereAnd = whereAnd.concat(arrayify(queryString.where, true));

      // helper method for collecting validation errors
      const addValidationError = function(message, data) {
         validationErrors.push({ message : message, data : data });
      };

      // helper methods for converting data types
      const dataTypeConverters = {};
      dataTypeConverters[Query2Query.types.INTEGER] = function(fieldName, valStr) {
         const val = Number.parseInt(valStr);
         if (isNaN(val)) {
            addValidationError("Failed to convert the value '" + valStr + "' of field '" + fieldName + "' to an integer");
         }
         return val;
      };
      dataTypeConverters[Query2Query.types.NUMBER] = function(fieldName, valStr) {
         const val = Number.parseFloat(valStr);
         if (isNaN(val)) {
            addValidationError("Failed to convert the value '" + valStr + "' of field '" + fieldName + "' to a number");
         }
         return val;
      };
      dataTypeConverters[Query2Query.types.BOOLEAN] = function(fieldName, valStr) {
         return toBoolean(valStr);
      };

      dataTypeConverters[Query2Query.types.DATETIME] = function(fieldName, valStr) {
         // if the string contains a colon, assume it's something that Date.parse() can handle
         let millis;
         if (valStr && valStr.indexOf(':') >= 0) {
            millis = Date.parse(valStr);
         }
         else {
            millis = Number.parseFloat(valStr);
         }
         if (isNaN(millis)) {
            addValidationError("Failed to convert the value '" + valStr + "' of field '" + fieldName + "' to a datetime");
            return millis;
         }

         return new Date(millis);
      };

      // validate the WHERE join
      whereJoin = whereJoin[0].toUpperCase();
      if (VALID_WHERE_JOINS.indexOf(whereJoin) < 0) {
         addValidationError("Invalid whereJoin value '" + whereJoin + "'.  Must be one of: " + VALID_WHERE_JOINS, { whereJoin : whereJoin });
      }

      // parse the SELECT fields
      let selectFields = processTokens(fields, allowedSelectFields);
      if (selectFields.length <= 0) {
         selectFields = selectFields.concat(allowedSelectFieldsArray);
      }

      // parse the WHERE expressions
      const whereExpressions = [];
      const whereValues = [];
      const processWhereExpressions = function(groups, joinTerm) {
         groups.forEach(function(expressionsGroup) {
            const expressions = expressionsGroup.split(TOKEN_SEPARATOR);
            const parsedExpressions = processTokens(expressions,
                                                    allowedWhereFields,
                                                    function(expression) {
                                                       const expressionParts = expression.split(WHERE_OPERERATORS_REGEX);
                                                       if (expressionParts.length === 3) {

                                                          const field = expressionParts[0].trim();

                                                          // first see whether this is even a field we should bother considering
                                                          if (field in allowedWhereFields) {
                                                             let operator = expressionParts[1].trim();
                                                             let value = expressionParts[2].trim();

                                                             if (value.toUpperCase() === NULL_VALUE) {
                                                                value = null;

                                                                // see whether this field's value is allowed to be null
                                                                if (allowNullValue[field]) {
                                                                   if (operator === '=') {
                                                                      operator = "IS"
                                                                   }
                                                                   else if (operator === '<>') {
                                                                      operator = "IS NOT"
                                                                   }
                                                                   else {
                                                                      addValidationError("Invalid WHERE operator '" + operator + "' when comparing with NULL.  Must be '=' or '<>'.");
                                                                   }
                                                                }
                                                                else {
                                                                   addValidationError("Field '" + field + "' cannot be compared with NULL", { field : field });
                                                                }
                                                             }
                                                             else {
                                                                // value isn't NULL, so now check whether it's of the correct data type
                                                                if (field in dataTypes) {
                                                                   value = dataTypeConverters[dataTypes[field]](field, value);
                                                                }
                                                             }

                                                             whereValues.push(value);
                                                             return {
                                                                field : field,
                                                                expression : "(" + [field, operator, '?'].join(' ') + ")"
                                                             };
                                                          }
                                                       }
                                                       return null;
                                                    },
                                                    true);
            if (parsedExpressions.length > 0) {
               const parsedExpressionsStr = parsedExpressions.join(" " + joinTerm + " ");
               if (parsedExpressions.length === 1) {
                  whereExpressions.push(parsedExpressionsStr);
               }
               else {
                  whereExpressions.push("(" + parsedExpressionsStr + ")")
               }
            }
         });
      };
      processWhereExpressions(whereAnd, WHERE_JOIN_AND);
      processWhereExpressions(whereOr, WHERE_JOIN_OR);

      // see if there where validation errors
      if (validationErrors.length > 0) {
         throw new JSendClientValidationError("Query Validation Error", validationErrors);
      }

      // build the ORDER BY fields
      const orderByFields = processTokens(orderBy, allowedOrderByFields, function(token) {
         const fieldAndExpression = {
            field : token,
            expression : token
         };

         // if the token starts with a dash, then we want the expression to be "[FIELD] DESC"
         if (token.indexOf('-') === 0) {
            fieldAndExpression.field = token.slice(1).trim();   // trim off the dash, leaving us with just the field
            fieldAndExpression.expression = fieldAndExpression.field + " DESC"
         }

         return fieldAndExpression;
      });

      // finally, build the various parts of the query
      const select = selectFields.join(',');
      const selectClause = "SELECT " + select;

      const where = whereExpressions.join(' ' + whereJoin + ' ');
      const whereClause = (whereValues.length > 0) ? "WHERE " + where : '';

      const orderByStr = orderByFields.join(',');
      const orderByClause = (orderByFields.length > 0) ? "ORDER BY " + orderByStr : '';

      // noinspection JSUnusedGlobalSymbols
      return {
         select : select,
         selectClause : selectClause,
         selectFields : selectFields,

         where : where,
         whereClause : whereClause,
         whereExpressions : whereExpressions,
         whereValues : whereValues,
         whereJoin : whereJoin,

         orderBy : orderByStr,
         orderByClause : orderByClause,
         orderByFields : orderByFields,

         offset : offset,
         limit : limit,
         limitClause : "LIMIT " + offset + "," + limit,

         sql : function(tableName, willExcludeOffsetAndLimit) {
            willExcludeOffsetAndLimit = !!willExcludeOffsetAndLimit;

            const sqlParts = [this.selectClause, "FROM " + tableName, this.whereClause, this.orderByClause];
            if (!willExcludeOffsetAndLimit) {
               sqlParts.push(this.limitClause);
            }
            return sqlParts.join(' ');
         }
      };
   };

   const arrayify = function(o, willNotProcessSubTokens) {
      willNotProcessSubTokens = !!willNotProcessSubTokens;
      const argType = typeof o;
      if (argType !== 'undefined' && o != null) {
         if (Array.isArray(o)) {
            // see if the array elements need to be split into tokens
            if (willNotProcessSubTokens) {
               return o;
            }
            else {
               let tokens = [];
               o.forEach(function(token) {
                  tokens = tokens.concat(token.split(TOKEN_SEPARATOR));
               });
               return tokens;
            }
         }

         if (argType === 'string') {
            return willNotProcessSubTokens ? [o] : o.split(TOKEN_SEPARATOR);
         }

         throw new Error("arrayify: Unexpected type: " + argType)
      }
      return [];
   };

   const parseIntAndEnforceBounds = function(str, defaultValue, min, max) {
      if (typeof str === 'string' || typeof str === 'number') {
         let num = parseInt(str, 10);
         num = isNaN(num) ? defaultValue : num;
         return Math.min(Math.max(min, num), max);
      }
      return defaultValue;
   };

   const processTokens = function(tokens, allowedFields, tokenProcessor, willAllowFieldMultiples) {
      // use the default token processor if undefined
      if (typeof tokenProcessor !== 'function') {
         tokenProcessor = DEFAULT_TOKEN_PROCESSOR;
      }

      willAllowFieldMultiples = !!willAllowFieldMultiples;

      // array for storing the created expressions
      const expressions = [];

      // the map helps us keep track of which fields we've already considered (we don't want to allow dupes)
      const fieldMap = {};

      // process the tokens into expressions
      tokens.forEach(function(token) {
         token = token.trim();
         if (token.length > 0) {
            // process the token into the base field and the associated expression
            const fieldAndExpression = tokenProcessor(token);

            if (fieldAndExpression) {
               // get the field
               const field = fieldAndExpression.field;

               // have we already considered this field?
               if (willAllowFieldMultiples || !(field in fieldMap)) {
                  // is this an allowed field?
                  if (field in allowedFields) {
                     expressions.push(fieldAndExpression.expression);
                  }

                  // remember this field so we don't consider it again if willAllowFieldMultiples is false
                  fieldMap[field] = true;
               }
            }
         }
      });

      return expressions;
   };
}

Query2Query.types = Object.freeze({
                                     INTEGER : 'INTEGER',
                                     NUMBER : 'NUMBER',
                                     STRING : 'STRING',
                                     DATETIME : 'DATETIME',
                                     BOOLEAN : 'BOOLEAN'
                                  });

module.exports = Query2Query;