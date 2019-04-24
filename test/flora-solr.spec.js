/* global describe, it, beforeEach, afterEach, after */

'use strict';

const chai = require('chai');
const { expect } = chai;
const _ = require('lodash');
const nock = require('nock');
const http = require('http');
const sinon = require('sinon');
const sandbox = require('sinon-test')(sinon);

const FloraSolr = require('../index');

const { ImplementationError } = require('flora-errors');

chai.use(require('sinon-chai'));

describe('Flora SOLR DataSource', () => {
    const solrUrl = 'http://example.com';
    const solrIndexPath = '/solr/article/select';
    const testResponse = '{"response":{"numFound":0,"docs":[]}}';
    let dataSource;
    let req;

    const api = {};

    beforeEach(() => {
        dataSource = new FloraSolr(api, {
            servers: {
                default: { urls: ['http://example.com/solr/'] }
            }
        });
    });

    afterEach(() => {
        if (req) req.done();
        nock.cleanAll();
    });

    after(() => nock.restore());

    describe('interface', () => {
        it('should export a query function', () => {
            expect(dataSource.process).to.be.a('function');
        });

        it('should export a prepare function', () => {
            expect(dataSource.prepare).to.be.a('function');
        });
    });

    it('should send requests using POST method', () => {
        req = nock(solrUrl)
            .post(solrIndexPath)
            .reply(200, testResponse);

        dataSource.process({ collection: 'article' });
    });

    it('should set content type to "application/x-www-form-urlencoded"', () => {
        req = nock(solrUrl)
            .matchHeader('content-type', 'application/x-www-form-urlencoded')
            .post(solrIndexPath)
            .reply(200, testResponse);

        dataSource.process({ collection: 'article' });
    });

    it('should set response format to JSON', () => {
        req = nock(solrUrl)
            .post(solrIndexPath, /wt=json/)
            .reply(200, testResponse);

        dataSource.process({ collection: 'article' });
    });

    it('should use "default" if no explicit server is specified', () => {
        req = nock(solrUrl)
            .post(solrIndexPath)
            .reply(200, testResponse);

        dataSource.process({ collection: 'article' });
    });

    it('should support multiple servers', done => {
        let archiveReq, awesomeReq, requests;
        const floraRequests = [{ collection: 'archive' }, { server: 'other', collection: 'awesome-index' }];
        const ds = new FloraSolr(api, {
            servers: {
                default: { urls: ['http://article.example.com/solr/'] },
                other: { urls: ['http://other.example.com/solr/'] }
            }
        });

        archiveReq = nock('http://article.example.com')
            .post('/solr/archive/select')
            .reply(200, testResponse);

        awesomeReq = nock('http://other.example.com')
            .post('/solr/awesome-index/select')
            .reply(200, testResponse);

        requests = floraRequests.map(request => ds.process(request));

        // make sure requests are triggered
        // nock requests trigger an exception when done() is called and request to url was not triggered
        Promise.all(requests)
            .then(() => {
                archiveReq.done();
                awesomeReq.done();
                done();
            }, done)
            .catch(done);
    });

    describe('error handling', () => {
        it('should trigger error if status code >= 400', done => {
            req = nock(solrUrl)
                .post(solrIndexPath)
                .reply(500, '{}');

            dataSource.process({ collection: 'article' }).catch(() => {
                done();
            });
        });

        it('should trigger error if response cannot be parsed', done => {
            req = nock(solrUrl)
                .post(solrIndexPath)
                .reply(418, '<p>Something went wrong</p>');

            dataSource.process({ collection: 'article' }).catch(() => {
                done();
            });
        });

        it("should handle request's error event", done => {
            // nock can't fake request errors at the moment, so we have to make a real request to nonexistent host
            dataSource = new FloraSolr(api, {
                servers: {
                    default: { urls: ['http://doesnotexists.localhost/solr/'] }
                }
            });

            dataSource.process({ collection: 'article' }).catch(err => {
                expect(err.code).to.equal('ECONNREFUSED');
                done();
            });
        });

        it('should trigger an error for non-existent server', done => {
            dataSource.process({ server: 'non-existent', collection: 'article' }).catch(err => {
                expect(err.message).to.contain('Server "non-existent" not defined');
                done();
            });
        });
    });

    describe('attributes', () => {
        it('should set requested attributes', () => {
            req = nock(solrUrl)
                .post(solrIndexPath, /fl=id%2Cname%2Cdate/)
                .reply(200, testResponse);

            dataSource.process({ collection: 'article', attributes: ['id', 'name', 'date'] });
        });
    });

    describe('filters', () => {
        it('should send "*:*" if no filter is set', () => {
            req = nock(solrUrl)
                .post(solrIndexPath, _.matches({ q: '*:*' }))
                .reply(200, testResponse);

            dataSource.process({ collection: 'article' });
        });

        // test conversion of boolean values
        _({ false: 0, true: 1 }).forEach((conversionTarget, booleanValue) => {
            it('should transform boolean ' + booleanValue + ' value to ' + conversionTarget + '', () => {
                const request = {
                    collection: 'article',
                    filter: [[{ attribute: 'foo', operator: 'equal', value: booleanValue !== 'false' }]]
                };

                req = nock(solrUrl)
                    .post(solrIndexPath, _.matches({ q: `(foo:${conversionTarget})` }))
                    .reply(200, testResponse);

                dataSource.process(request);
            });
        });

        it('should support arrays', () => {
            const request = {
                collection: 'article',
                filter: [[{ attribute: 'foo', operator: 'equal', value: [1, 3, 5, 7] }]]
            };

            req = nock(solrUrl)
                .post(solrIndexPath, _.matches({ q: '(foo:(1 OR 3 OR 5 OR 7))' }))
                .reply(200, testResponse);

            dataSource.process(request);
        });

        const specialChars = ['\\', '/', '+', '-', '&', '|', '!', '(', ')', '{', '}', '[', ']', '^', '"', '~', '*', '?', ':'];
        specialChars.forEach(character => {
            it('should escape special character ' + character, () => {
                const request = {
                    collection: 'article',
                    filter: [[{ attribute: 'foo', operator: 'equal', value: character + 'bar' }]]
                };

                req = nock(solrUrl)
                    .post(solrIndexPath, _.matches({ q: `(foo:\\${character}bar)` }))
                    .reply(200, testResponse);

                dataSource.process(request);
            });
        });

        describe('range queries', () => {
            it('should support fixed lower boundaries', () => {
                const request = {
                    collection: 'article',
                    filter: [[{ attribute: 'foo', operator: 'greaterOrEqual', value: 1337 }]]
                };

                req = nock(solrUrl)
                    .post(solrIndexPath, _.matches({ q: '(foo:[1337 TO *])' }))
                    .reply(200, testResponse);

                dataSource.process(request);
            });

            it('should support fixed upper boundaries', () => {
                const request = {
                    collection: 'article',
                    filter: [[{ attribute: 'foo', operator: 'lessOrEqual', value: 1337 }]]
                };

                req = nock(solrUrl)
                    .post(solrIndexPath, _.matches({ q: '(foo:[* TO 1337])' }))
                    .reply(200, testResponse);

                dataSource.process(request);
            });

            it('should support fixed lower and upper boundaries', () => {
                const request = {
                    collection: 'article',
                    filter: [
                        [
                            { attribute: 'foo', operator: 'greaterOrEqual', value: 1337 },
                            { attribute: 'foo', operator: 'lessOrEqual', value: 4711 }
                        ]
                    ]
                };

                req = nock(solrUrl)
                    .post(solrIndexPath, _.matches({ q: '(foo:[1337 TO 4711])' }))
                    .reply(200, testResponse);

                dataSource.process(request);
            });
        });

        it('should transform single filters', () => {
            const request = {
                collection: 'article',
                filter: [[{ attribute: 'foo', operator: 'equal', value: 'foo' }]]
            };

            req = nock(solrUrl)
                .post(solrIndexPath, _.matches({ q: '(foo:foo)' }))
                .reply(200, testResponse);

            dataSource.process(request);
        });

        it('should transform complex filters', () => {
            const request = {
                collection: 'article',
                search: 'foo bar',
                filter: [
                    [{ attribute: 'authorId', operator: 'equal', value: 1337 }, { attribute: 'typeId', operator: 'equal', value: 4711 }],
                    [{ attribute: 'status', operator: 'equal', value: 'future' }]
                ]
            };

            req = nock(solrUrl)
                .post(solrIndexPath, _.matches({ q: 'foo bar AND ((authorId:1337 AND typeId:4711) OR (status:future))' }))
                .reply(200, testResponse);

            dataSource.process(request);
        });

        it('should support composite key filters', () => {
            const request = {
                collection: 'awesome_index',
                filter: [
                    [
                        {
                            attribute: ['intKey', 'stringKey', 'boolKey'],
                            operator: 'equal',
                            // test string escaping and boolean conversion
                            value: [[1337, '(foo)', true], [4711, 'bar!', false]]
                        }
                    ]
                ]
            };

            req = nock(solrUrl)
                .post(
                    '/solr/awesome_index/select',
                    _.matches({
                        q: '((intKey:1337 AND stringKey:\\(foo\\) AND boolKey:1) OR (intKey:4711 AND stringKey:bar\\! AND boolKey:0))'
                    })
                )
                .reply(200, testResponse);

            dataSource.process(request);
        });

        const supportedFilters = {
            equal: '(date:2015\\-12\\-31)',
            greaterOrEqual: '(date:[2015\\-12\\-31 TO *])',
            lessOrEqual: '(date:[* TO 2015\\-12\\-31])',
            notEqual: '(-date:2015\\-12\\-31)'
        };
        Object.keys(supportedFilters).forEach(operator => {
            it('should support "' + operator + '" filters', () => {
                const request = {
                    collection: 'article',
                    filter: [[{ attribute: 'date', operator: operator, value: '2015-12-31' }]]
                };

                req = nock(solrUrl)
                    .post('/solr/article/select', _.matches({ q: supportedFilters[operator] }))
                    .reply(200, testResponse);

                dataSource.process(request);
            });
        });

        ['less', 'greater'].forEach(operator => {
            it('should trigger an error for unsupported filter operator "' + operator + '"', done => {
                const request = {
                    collection: 'article',
                    filter: [[{ attribute: 'date', operator: operator, value: '2015-12-31' }]]
                };

                dataSource.process(request).catch(err => {
                    expect(err).to.be.instanceOf(ImplementationError);
                    expect(err.message).to.contain('not support "' + operator + '" filters');
                    done();
                });
            });
        });

        it('should append additional query parameters', () => {
            const request = {
                collection: 'awesome_index',
                queryAddition: '_val_:"product(assetClassBoost,3)"\n_val_:"product(importance,50)"'
            };

            req = nock(solrUrl)
                .post('/solr/awesome_index/select', _.matches({ q: '_val_:"product(assetClassBoost,3)" _val_:"product(importance,50)"' }))
                .reply(200, testResponse);

            dataSource.process(request);
        });
    });

    describe('full-text search', () => {
        it('should add search term to query', () => {
            const request = {
                collection: 'article',
                search: 'fo(o)bar'
            };

            req = nock(solrUrl)
                .post(solrIndexPath, _.matches({ q: 'fo\\(o\\)bar' }))
                .reply(200, testResponse);

            dataSource.process(request);
        });

        it('should support additional filter(s)', () => {
            const request = {
                collection: 'article',
                search: 'foo bar',
                filter: [[{ attribute: 'authorId', operator: 'equal', value: 1337 }]]
            };

            req = nock(solrUrl)
                .post(solrIndexPath, _.matches({ q: 'foo bar AND (authorId:1337)' }))
                .reply(200, testResponse);

            dataSource.process(request);
        });
    });

    describe('order', () => {
        it('single criterion', () => {
            const request = {
                collection: 'article',
                order: [{ attribute: 'foo', direction: 'asc' }]
            };

            req = nock(solrUrl)
                .post(solrIndexPath, /sort=foo%20asc/)
                .reply(200, testResponse);

            dataSource.process(request);
        });

        it('multiple criteria', () => {
            const request = {
                collection: 'article',
                order: [{ attribute: 'foo', direction: 'asc' }, { attribute: 'bar', direction: 'desc' }]
            };

            req = nock(solrUrl)
                .post(solrIndexPath, /sort=foo%20asc%2Cbar%20desc/)
                .reply(200, testResponse);

            dataSource.process(request);
        });
    });

    describe('pagination', () => {
        it('should set limit', () => {
            req = nock(solrUrl)
                .post(solrIndexPath, /rows=15/)
                .reply(200, testResponse);

            dataSource.process({ collection: 'article', limit: 15 });
        });

        it('should overwrite SOLR default limit for sub-resource processing', () => {
            req = nock(solrUrl)
                .post(solrIndexPath, _.matches({ rows: '1000000' }))
                .reply(200, testResponse);

            // no explicit limit set
            dataSource.process({ collection: 'article' });
        });

        it('should set page', () => {
            req = nock(solrUrl)
                // only return sorted pagination params because they can appear in any order
                .filteringRequestBody(body => {
                    const params = require('querystring').parse(body);
                    const paginationParams = [];

                    ['rows', 'start'].forEach(key => {
                        // extract pagination params
                        if (params[key]) {
                            paginationParams.push(key + '=' + params[key]);
                        }
                    });

                    return paginationParams.sort().join('&');
                })
                .post(solrIndexPath, /rows=10&start=20/)
                .reply(200, testResponse);

            dataSource.process({ collection: 'article', limit: 10, page: 3 });
        });
    });

    describe('timeout', () => {
        describe('defaults', () => {
            it(
                'should set connect timeout to 2 seconds',
                sandbox(function(done) {
                    const requestSpy = this.spy(http, 'request');

                    req = nock(solrUrl)
                        .post(solrIndexPath)
                        .reply(200, testResponse);

                    dataSource.process({ collection: 'article' }).then(() => {
                        expect(requestSpy).to.have.been.calledWith(sinon.match.has('timeout', 2000), sinon.match.func);
                        done();
                    });
                })
            );

            it('should set request timeout to 10 seconds', done => {
                req = nock(solrUrl)
                    .post(solrIndexPath)
                    .socketDelay(11000)
                    .reply(200, testResponse);

                dataSource.process({ collection: 'article' }).catch(err => {
                    expect(err.code).to.equal('ECONNRESET');
                    done();
                });
            });
        });

        describe('config options', () => {
            it(
                'should overwrite default connect timeout',
                sandbox(function(done) {
                    const TIMEOUT = 5000;
                    const requestSpy = this.spy(http, 'request');
                    const ds = new FloraSolr(api, {
                        servers: {
                            default: {
                                urls: ['http://example.com/solr/'],
                                connectTimeout: TIMEOUT
                            }
                        }
                    });

                    req = nock(solrUrl)
                        .post(solrIndexPath)
                        .socketDelay(10000)
                        .reply(200, testResponse);

                    ds.process({ collection: 'article' }).then(() => {
                        expect(requestSpy).to.have.been.calledWith(sinon.match.has('timeout', TIMEOUT), sinon.match.func);
                        done();
                    });
                })
            );

            it('should overwrite default request timeout', done => {
                const ds = new FloraSolr(api, {
                    servers: {
                        default: {
                            urls: ['http://example.com/solr/'],
                            requestTimeout: 1500
                        }
                    }
                });

                req = nock(solrUrl)
                    .post(solrIndexPath)
                    .socketDelay(3000)
                    .reply(200, testResponse);

                ds.process({ collection: 'article' }).catch(err => {
                    expect(err.code).to.equal('ECONNRESET');
                    done();
                });
            });
        });
    });

    describe('limitPer', () => {
        it('should activate result grouping', () => {
            req = nock(solrUrl)
                .post(solrIndexPath, _.matches({ group: { 0: 'true' } }))
                .reply(200, testResponse);

            dataSource.process({ collection: 'article', limitPer: 'seriesId' });
        });

        it('should use limitPer as group.field parameter', () => {
            req = nock(solrUrl)
                .post(solrIndexPath, _.matches({ group: { field: 'seriesId' } }))
                .reply(200, testResponse);

            dataSource.process({ collection: 'article', limitPer: 'seriesId' });
        });

        it('should return flat list instead of groups', () => {
            req = nock(solrUrl)
                .post(
                    solrIndexPath,
                    _.matches({
                        group: {
                            1: { format: 'simple' },
                            main: 'true'
                        }
                    })
                )
                .reply(200, testResponse);

            dataSource.process({ collection: 'article', limitPer: 'seriesId' });
        });

        it('should set limit', () => {
            req = nock(solrUrl)
                .post(solrIndexPath, _.matches({ group: { limit: '3' }, rows: '1000000' }))
                .reply(200, testResponse);

            dataSource.process({ collection: 'article', limitPer: 'seriesId', limit: 3 });
        });

        it('should not set group sort order', () => {
            req = nock(solrUrl)
                .post(solrIndexPath, body => {
                    const { group } = body;
                    return group && typeof group === 'object' && !group.hasOwnProperty('sort');
                })
                .reply(200, testResponse);

            dataSource.process({
                collection: 'article',
                limitPer: 'seriesId',
                order: [{ attribute: 'date', direction: 'desc' }]
            });
        });
    });
});
