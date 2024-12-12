/**
 * This script is the main entry point for the Beckhoff to MQTT integration
 * microservice. It reads in a configuration file, uses that configuration to
 * connect to a Beckhoff PLC, read data from it, and publish the data to an MQTT
 * broker.
 * 
 * @author [IOT Academy Cohort 3 Group 2 Team 2](https://github.com/IOT-Academy-Cohort-3-Group-2)
 * @version 1.0
 */

import * as ads from 'ads-client';
import * as mqtt from 'mqtt';
import * as fs from 'fs'; 
import { Config } from './config-interface';


/**
 * Reads a file synchronously. If the file is read successfully, its
 * contents will be returned as a string. If the file can't be read (for
 * example, if the path is invalid), this function will return a void
 * value.
 * @param file_path The path to the file to read
 * @returns The contents of the file as a string, or void if the file
 *          can't be read
 */
function readFile(file_path: string): string | void { 
    try { 
        const data = fs.readFileSync(file_path, 'utf8'); 
        return data; 
    } catch (err) { 
        console.error(err); 
        return; 
    }
}

/**
 * Main function that orchestrates the data fetching and publishing process.
 * 
 * This function reads in a configuration file, uses that configuration to
 * connect to a Beckhoff PLC, read data from it, and publish the data to an MQTT
 * broker. It also handles reconnections to the PLC and MQTT broker if the
 * connections are lost.
 * 
 * @author [IOT Academy Cohort 3 Group 2 Team 2](https://github.com/IOT-Academy-Cohort-3-Group-2)
 * @version 1.0
 */
async function main() {  
    //where the config.json structure will live
    let config: Config;

    console.log('Loading Configurations...');
    //read the config.json file
    const config_str:string|void = readFile('config.json');
    //if it's not string (it's a void) OR it's empty, then we can't go on 
    if(typeof config_str !== "string" || config_str.length === 0){
        console.error('Failed to parse config.json. (empty file or null returned)  Application Aborted.');
        return;
    } else {
        //attempting to JSON parse this file's contents should load it into memory as the Config interface.  
        //So, if parsing fails we can't go on
        try {
            //parsing succeeded, we're in business
            config = JSON.parse(config_str) as Config;
            console.log("Configurations Successfully Loaded.");
        } catch(err) {
            //parsing failed. we go bye bye... :(
            console.error('Failed to parse config.json. (failed to parse as a json object)  Application Aborted.')
            return;
        }
    }
    //now config is loaded, use it....
    console.log('Starting PLC to MQTT integration...');  
    //connect to MQTT using config settings
    console.log('MQTT Connection');
    const mqttClient = mqtt.connect(config.mqtt.connection.brokerUrl, config.mqtt.connection.options);  
    mqttClient.on('connect', () => {    
        console.log('Connected to MQTT broker');  
    });  
    mqttClient.on('error', (err) => {    
        console.error('MQTT connection error:', err);  
    });  
    //instantiate the plc client using config settings
    const adsClient = new ads.Client(config.plc.connection);  
    try {    
        console.log('PLC Connection')
        // Connect to Beckhoff PLC    
        await adsClient.connect();    
        console.log('Connected to Beckhoff PLC');    
        // Loop forever on an interval (interval in ms is found in config.json)
        setInterval(async () => {      
            try {        
                //using the config.plc.tags sub-groupings in the config as the payload definition,
                //traverse each group as a payload to build
                for(const payload_group in config.plc.tags) {                    
                    if (config.plc.tags.hasOwnProperty(payload_group)) {
                        //build empty object
                        const payload: { [key: string]: any } = {};
                        //get all the tags for this payload
                        for (const tag of config.plc.tags[payload_group]) { 
                            // Get value 
                            const key: string = tag.description; 
                            const value: ads.SymbolData = await adsClient.readSymbol(tag.tagname); 
                            // Add to object 
                            payload[key] = value.value; 
                        }          

                        // Publish object to MQTT 
                        // Map topic as basetopic/joined(topic_mapping)/groupname 
                        const joinedMapping = config.mqtt.topic_mapping .map(mapping => Object.values(mapping)[0]) .join('/'); 
                        const topic = `${config.mqtt.connection.baseTopic}${joinedMapping}/${payload_group}`; 
                        //jsonify the payload by splatting it
                        const payloadString = JSON.stringify({ 
                            timestamp: new Date().toISOString(), 
                            ...payload }); 
                        //publish
                        mqttClient.publish(topic, payloadString, (err) => { 
                            if (err) { 
                                console.error(`Failed to publish to topic ${topic}:`, err); 
                            } else { 
                                console.log(`Published to ${topic}:`, payloadString); 
                            } 
                        });
                    }
                }
            }
            catch (err) {        
                console.error('Error reading ADS data or publishing to MQTT:', err);      
            }    
        },config.mqtt.connection.polling_interval); 
        } catch (err) {    
            console.error('Error connecting to Beckhoff PLC:', err);  
        } finally {    
            // Graceful shutdown - probably will never happen but 
            // programmers are too OCD not to code an ending
            process.on('SIGINT', async () => {      
                console.log('Disconnecting services...');      
                await adsClient.disconnect();      
                mqttClient.end();      
                console.log('Disconnected ADS and MQTT clients. Exiting.');      
                process.exit(0);    
            });  
        }
    }
        
    main();
