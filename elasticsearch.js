/*
 * Copyright (c) 2019 ARM Limited. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 * Licensed under the Apache License, Version 2.0 (the License); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
'use strict';

const path = require('path');
const AWS = require('aws-sdk');
 
const AWS_REGION = process.env.AWS_REGION;
const ELASTICSEARCH_DOMAIN = process.env.ELASTICSEARCH_DOMAIN;

const endpoint = new AWS.Endpoint(ELASTICSEARCH_DOMAIN);
const httpClient = new AWS.HttpClient();
const credentials = new AWS.EnvironmentCredentials('AWS');


/**
 * Sends a request to Elasticsearch
 *
 * @param {string} httpMethod - The HTTP method, e.g. 'GET', 'PUT', 'DELETE', etc
 * @param {string} requestPath - The HTTP path (relative to the Elasticsearch domain), e.g. '.kibana'
 * @param {Object} [payload] - An optional JavaScript object that will be serialized to the HTTP request body
 * @returns {Promise} Promise - object with the result of the HTTP response
 */
const sendRequest = ({ httpMethod, requestPath, payload }) => {

    /**
     * Construct new HTTP request to Elasticsearch in same region.
     */
    const request = new AWS.HttpRequest(endpoint, AWS_REGION);
 
    request.method = httpMethod;
    request.path = path.join(request.path, requestPath);
    request.body = payload;
    request.headers['Content-Type'] = 'application/json';
    request.headers['Host'] = ELASTICSEARCH_DOMAIN;
 
//    console.log(request);

    /**
     * Sign request.
     */
    const signer = new AWS.Signers.V4(request, 'es');
    signer.addAuthorization(credentials, new Date());

//    console.log(request);

    /**
     * Return promise for async request.
     */
    return new Promise((resolve, reject) => {
        httpClient.handleRequest(request, null,
            response => {

                const { statusCode, statusMessage, headers } = response;

                /* hold partial data. */
                let body = '';
                
                /* accumulate data chunks. */
                response.on('data', chunk => {
                    body += chunk;
                });
                
                /* on request end, resolve promise. */
                response.on('end', () => {
                    const data = {
                        statusCode,
                        statusMessage,
                        headers
                    };
                    if (body) {
                        data.body = JSON.parse(body);
                    }
                    resolve(data);
                });
            },
            err => {
                reject(err);
            });
    });
}

/**
 * Public functions for sending bulk data.
 */
module.exports = {

    /**
     * Send bulk insertion request for device notifications.
     */
    sendNotifications: (notifications, done) => {
        
        let bulk = '';
        
        /* iterate through notifications. */
        for (const item of notifications) {

//        console.log(item);

            /* decode base64 encoded resource value. */
            const value = Buffer.from(item.payload, 'base64').toString('ascii');

            /* record to be inserted into elasticsearch. */
            let record = {
                'time': Date.now(),
                'endpoint': item.ep,
                'path': item.path,
                'value': value 
            };

//            console.log(record);

            /* use bulk insertion. */
            bulk += '{"index":{"_index":"notifications","_type":"_doc"}}\n';
            bulk += JSON.stringify(record).trim() + '\n';                
        }
        
        const params = {
            httpMethod: 'POST',
            requestPath: '/_bulk',
            payload: bulk
        };

        sendRequest(params)
            .then(resolve => {
//                console.info(resolve);
                done(null, 'success');
            }, reject => {
                console.info(reject);
                done(reject);
        });
    },

    /**
     * Send bulk insertion request for device registrations.
     */
    sendRegistrations: (registrations, done) => {
        
        let bulk = '';
        
        /* iterate through registrations. */
        for (const item of registrations) {
            
            /* record with device information. */
            let record_device_info = {
                'time': Date.now(),
                'endpoint': item.ep,
                'original-ep': item["original-ep"],
                'ept': item.ept,
                'resources': JSON.stringify(item.resources)
            };
            
            /* bulk insert into devices. */
            bulk += '{"index":{"_index":"devices","_type":"_doc"}}\n';
            bulk += JSON.stringify(record_device_info).trim() + '\n';                

            /* record with device registration. */
            let record_device_registration = {
                'time': Date.now(),
                'endpoint': item.ep,
                'value': 1
            };
            
            /* bulk insert into devices. */
            bulk += '{"index":{"_index":"registrations","_type":"_doc"}}\n';
            bulk += JSON.stringify(record_device_registration).trim() + '\n';                

//            console.log(record_device_info);
//            console.log(record_device_registration);
//            console.log(bulk);
        }

        const params = {
            httpMethod: 'POST',
            requestPath: '/_bulk',
            payload: bulk
        };

        sendRequest(params)
            .then(resolve => {
//                console.info(resolve);
                done(null, 'success');
            }, reject => {
                console.info(reject);
                done(reject);
        });
    },

    /**
     * Send bulk insertion request for device expirations.
     */
    sendExpirations: (expirations, done) => {
        
        /* CloudWatch keyword: debug*/
        console.log('debug:', 'registrations-expired:', expirations);

        let bulk = '';
        
        /* iterate through deregistrations. */
        for (const item of expirations) {

            console.log(item);

            /* record with device deregistration. */
            let record_device_deregistration = {
                'time': Date.now(),
                'endpoint': item,
                'value': 0
            };
            
            /* bulk insert into devices. */
            bulk += '{"index":{"_index":"registrations","_type":"_doc"}}\n';
            bulk += JSON.stringify(record_device_deregistration).trim() + '\n';                

//            console.log(record_device_deregistration);
        }

        const params = {
            httpMethod: 'POST',
            requestPath: '/_bulk',
            payload: bulk
        };

        sendRequest(params)
            .then(resolve => {
//                console.info(resolve);
                done(null, 'success');
            }, reject => {
                console.info(reject);
                done(reject);
        });
    }
}
