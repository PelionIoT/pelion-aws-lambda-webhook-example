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
 
const db = require('./elasticsearch');

/**
 * Pelion webhook handler.
 * 
 * Inserts information from callback into Elasticsearch.
 */
exports.handler = (event, context, callback) => {
//    console.log('Received event:', JSON.stringify(event, null, 2));

    /**
     * Default function for returning REST API call.
     */
    const done = (err, res) => callback(null, {
        statusCode: err ? '400' : '200',
        body: err ? err.message : JSON.stringify(res),
        headers: {
            'Content-Type': 'application/json',
        },
    });

    /**
     * Pelion callbacks are PUT with payload in the body field.
     */ 
    if (event.httpMethod === 'PUT') {
        if (event.hasOwnProperty('body')) {

            /* Parse JSON body. */            
            const body = JSON.parse(event.body);

            /**
             * Callback contains notifications.
             */ 
            if (body.hasOwnProperty('notifications')) {
//                console.log('notifications');

                db.sendNotifications(body.notifications, done);

            /**
             * Callback contains registrations.
             */ 
            } else if (body.hasOwnProperty('registrations') || body.hasOwnProperty('reg-updates')) {
//                console.log('registrations');

                if (body.hasOwnProperty('registrations')) {
                    db.sendRegistrations(body.registrations, done);
                } else {
                    db.sendRegistrations(body["reg-updates"], done);
                }

            /**
             * Callback contains deregistrations. 
             */
            } else if (body.hasOwnProperty('registrations-expired')) {

                db.sendExpirations(body["registrations-expired"], done);

            /**
             * Unknown body. Ignore and return success to signal we acknowledge 
             * receiving the callback. 
             */ 
            } else {
                console.log('unknown type: ', event.body);

                done(null, 'success');
            }
        }
    }
    else {
        done(new Error(`Unsupported method "${event.httpMethod}"`));
    }
};
