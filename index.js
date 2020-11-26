'use strict';

const http = require('http');
const querystring = require('querystring');

const _ = require('lodash');
const { ImplementationError } = require('flora-errors');

const SUPPORTED_FILTERS = ['equal', 'notEqual', 'lessOrEqual', 'greaterOrEqual', 'range'];

const NO_LIMIT = 1000000;

/**
 * Return true if two filters on same attribute can be used as single range filter.
 *
 * @param {Array.<Object>} filters
 * @return {boolean}
 * @private
 */
function filterRangeQueries(filters) {
    if (filters.length !== 2) return false;

    const rangeOperators = ['lessOrEqual', 'greaterOrEqual'];
    const operator1 = filters[0].operator;
    const operator2 = filters[1].operator;

    return rangeOperators.includes(operator1) && rangeOperators.includes(operator2);
}

/**
 * Create range filter from a greatOrEqual and lessOrEqual filter.
 *
 * @param {Array.<string>} attributeFilters
 * @return {Object}
 * @private
 */
function createRangeFilter(attributeFilters) {
    const rangeFilter = { attribute: attributeFilters[0].attribute, operator: 'range' };

    // make sure greaterOrEqual filter comes first
    attributeFilters.sort((filter) => (filter.operator === 'greaterOrEqual' ? -1 : 1));

    rangeFilter.value = [attributeFilters[0].value, attributeFilters[1].value];
    return rangeFilter;
}

/**
 * Convert greaterOrEqual and lessOrEqual filters on same attribute
 * to a single range filter.
 *
 * @param {Array.<Object>} filters
 * @return {Array.<Object>}
 * @private
 */
function rangify(filters) {
    const hasRangeableFilters = filters.some((filter) => ['lessOrEqual', 'greaterOrEqual'].includes(filter.operator));
    if (!hasRangeableFilters) {
        return filters;
    }

    const groupedAttrs = _.groupBy(filters, 'attribute');
    const rangeQueries = _.filter(groupedAttrs, filterRangeQueries);

    if (!rangeQueries.length) return filters;

    const rangeQueryAttrs = rangeQueries.map((rangeQuery) => rangeQuery[0].attribute);

    // copy non-range query attributes
    return filters.filter((filter) => !rangeQueryAttrs.includes(filter.attribute)).concat(rangeQueries.map(createRangeFilter));
}

/**
 * @param {string} value
 * @return {string}
 * @private
 */
function escapeSpecialChars(value) {
    const reservedKeywordRegex = /\b(AND|NOT|OR)\b/g;

    if (reservedKeywordRegex.test(value)) {
        value = value.replace(reservedKeywordRegex, (keyword) => keyword.toLowerCase());
    }

    return value.replace(/([\\/+\-&|!(){}[\]^"~*?:])/g, '\\$1');
}

/**
 * @param {string} value
 * @return {string}
 * @private
 */
function escapeSpecialCharsSolrSyntax(value) {
    return value.replace(/([\\/+\-&|!(){}[\]^~?])/g, '\\$1');
}

/**
 * Escape strings and convert boolean values.
 *
 * @param {*} value
 * @param {boolean} exposeSolrSyntax
 * @return {*}
 * @private
 */
function escapeValueForSolr(value, exposeSolrSyntax) {
    if (typeof value === 'string') {
        value = exposeSolrSyntax ? escapeSpecialCharsSolrSyntax(value) : escapeSpecialChars(value);
    } else if (typeof value === 'boolean') value = value === false ? 0 : 1;
    return value;
}

/**
 * @param {Object} filter
 * @return {string}
 * @private
 */
function convertFilterToSolrSyntax(filter) {
    const { operator } = filter;
    let { value } = filter;

    if (!SUPPORTED_FILTERS.includes(filter.operator)) {
        throw new ImplementationError(`DataSource "flora-solr" does not support "${operator}" filters`);
    }

    if (!Array.isArray(filter.attribute)) {
        value = escapeValueForSolr(value);
        if (Array.isArray(value)) {
            if (operator === 'range') value = '[' + value[0] + ' TO ' + value[1] + ']';
            else value = '(' + value.join(' OR ') + ')';
        }

        if (operator !== 'notEqual') {
            if (operator === 'greaterOrEqual') value = '[' + value + ' TO *]';
            if (operator === 'lessOrEqual') value = '[* TO ' + value + ']';
            return filter.attribute + ':' + value;
        }
        return '-' + filter.attribute + ':' + value;
    }

    // convert composite keys to SOLR syntax
    return value
        .map((values) => {
            const conditions = values.map((val, index) => filter.attribute[index] + ':' + escapeValueForSolr(val));
            return '(' + conditions.join(' AND ') + ')';
        })
        .join(' OR ');
}

function buildSolrFilterString(floraFilters) {
    const orConditions = floraFilters.map((andFilters) => {
        if (andFilters.length > 1) andFilters = rangify(andFilters);
        const conditions = andFilters.map(convertFilterToSolrSyntax);
        return '(' + conditions.join(' AND ') + ')';
    });

    if (orConditions.length > 1) return '(' + orConditions.join(' OR ') + ')';

    return orConditions.join('');
}

/**
 * @param {Array.<Object>} floraOrders
 * @return {string}
 * @private
 */
function buildSolrOrderString(floraOrders) {
    return floraOrders.map((order) => order.attribute + ' ' + order.direction).join(',');
}

function parseData(str) {
    try {
        return JSON.parse(str);
    } catch (e) {
        return new Error('Could not parse response: ' + str);
    }
}

function prepareQueryAddition(queryAdditions) {
    return queryAdditions
        .replace(/[\r\n]+/g, ' ')
        .replace(/\s{2,}/g, ' ')
        .trim();
}

/**
 * @param {Object} servers
 * @return {Object}
 */
function getUrlGenerators(servers) {
    return Object.keys(servers).reduce((acc, server) => {
        const { urls } = servers[server];

        acc[server] = (function* urlGenerator() {
            while (true) {
                for (let i = 0, l = urls.length; i < l; ++i) {
                    yield urls[i];
                }
            }
        })();

        return acc;
    }, {});
}

/**
 *
 * @param {string} requestUrl
 * @param {Object} params
 * @param {Object} requestOptions
 * @param {Agent} agent
 * @returns {Promise}
 * @private
 */
function querySolr(requestUrl, params, requestOptions, agent) {
    return new Promise((resolve, reject) => {
        const options = {
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            timeout: requestOptions.connectTimeout,
            agent,
        };

        const req = http.request(requestUrl, options, (res) => {
            const chunks = [];

            res.on('data', (chunk) => chunks.push(chunk));

            res.on('end', () => {
                const data = parseData(Buffer.concat(chunks).toString('utf8'));

                if (res.statusCode >= 400 || data instanceof Error) {
                    const error = new Error(`Solr error: ${res.statusCode} ${http.STATUS_CODES[res.statusCode]}`);
                    return reject(error);
                }

                return resolve({ totalCount: data.response.numFound, data: data.response.docs });
            });
        });

        req.setTimeout(requestOptions.requestTimeout, () => req.abort());

        req.write(querystring.stringify(params)); // add params to POST body

        req.on('error', (err) => {
            err.message = `Solr error: ${err.message} (${options.host})`;
            reject(err);
        });

        req.end();
    });
}

function prepareSearchTerm(request) {
    const escapedSearchTerm = escapeValueForSolr(request.search, request.exposeSolrSyntax);
    const allowedSearchFields =
        request.allowedSearchFields && request.allowedSearchFields.trim() ? request.allowedSearchFields.trim().split(',') : [];

    if (!allowedSearchFields.length) {
        return escapedSearchTerm;
    }

    const regex = new RegExp(`^(?<field>${allowedSearchFields.join('|')}):(?<search>.+)`);
    const match = regex.exec(request.search);

    if (match === null) {
        return escapedSearchTerm;
    }

    return `(${match.groups.field}:"${match.groups.search}")`;
}

class DataSource {
    /**
     * @param {Api} api
     * @param {Object} config
     */
    constructor(api, config) {
        this.options = config;
        this._urls = getUrlGenerators(config.servers);
        this._status = config._status;
        delete config._status;

        this._agent = new http.Agent({
            maxSockets: 5,
            keepAlive: true,
            keepAliveMsecs: 10000,
        });
    }

    /**
     * @public
     */
    prepare() {}

    /**
     * @param {Object} request
     * @returns {Promise<Object>}
     */
    async process(request) {
        const server = request.server || 'default';
        const queryParts = [];
        const params = { wt: 'json' };
        const serverOpts = this.options.servers;

        if (!serverOpts[server]) throw new Error(`Server "${server}" not defined`);

        const requestUrl = this._urls[server].next().value + request.collection + '/select';

        if (request.attributes) params.fl = request.attributes.join(',');
        if (request.order) params.sort = buildSolrOrderString(request.order);

        if (request.df) params.df = request.df;
        if (request.search && request.search.trim()) queryParts.push(prepareSearchTerm(request));
        if (request.filter) {
            queryParts.push(buildSolrFilterString(request.filter));
        }
        if (request.queryAddition) queryParts.push(prepareQueryAddition(request.queryAddition));
        if (queryParts.length === 0) queryParts.push('*:*');

        // overwrite SOLR default limit for sub-resource processing
        if (!request.limit) request.limit = NO_LIMIT;
        if (request.page) params.start = (request.page - 1) * request.limit;

        if (!request.limitPer) params.rows = request.limit;
        else {
            Object.assign(params, {
                group: 'true',
                'group.format': 'simple',
                'group.main': 'true',
                'group.field': request.limitPer,
                'group.limit': request.limit,
                rows: NO_LIMIT, // disable default limit because groups are returned as list
            });
        }

        params.q = queryParts.join(' AND ');

        if (request._explain) Object.assign(request._explain, { url: requestUrl, params });
        if (this._status) this._status.increment('dataSourceQueries');

        const requestOpts = {
            connectTimeout: serverOpts[server].connectTimeout || 2000,
            requestTimeout: serverOpts[server].requestTimeout || 5000,
        };

        return querySolr(requestUrl, params, requestOpts, this._agent);
    }

    /**
     * @returns {Promise}
     */
    close() {
        return Promise.resolve();
    }

    /**
     * @param {...string} args
     */
    escape(...args) {
        return escapeValueForSolr(...args);
    }
}

module.exports = DataSource;
