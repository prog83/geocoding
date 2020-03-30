const fs = require('fs');
const csv = require('csv-parser');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const axios = require('axios');
const https = require('https');
const resultsRead = [];

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

async function geocoding(query) {
  if (query.length > 0) {
    const response = await axios.get(
      'https://nominatim.openstreetmap.org/search',
      {
        params: {
          format: 'json',
          addressdetails: '0',
          limit: 1,
          q: query.join(' '),
        },
        // httpsAgent: new https.Agent({
        //   rejectUnauthorized: false,
        // }),
        timeout: 10000,
      }
    );

    let result = null;
    const { data } = response;
    if (data.length === 0) {
      result = geocoding(query.slice(0, -1));
    } else {
      result = response;
    }
    return result;
  }
  return {};
}

console.log('...Start');

fs.createReadStream('csv.csv')
  .pipe(csv({ separator: ';' }))
  .on('data', data => resultsRead.push(data))
  .on('end', () => {
    const getData = () =>
      Promise.all(
        resultsRead.map(async (item, index) => {
          try {
            await sleep(index * 50);

            const query = item['Address'].split(' ').filter(i => i !== '');
            const { data = [] } = await geocoding(query);
            const { lat, lon } = data[0] || {};
            // console.log(item['﻿№'], lat, lon);
            return {
              ...item,
              lat,
              lon,
            };
          } catch (error) {
            console.log(error.message);
            return { ...item, error: error.message };
          }
        })
      );

    getData().then(data => {
      const csvWriter = createCsvWriter({
        path: 'geocoding.csv',
        header: [
          { id: '﻿№', title: '№' },
          { id: 'ПІБ', title: 'ПІБ' },
          { id: 'ПІБ (лат.)', title: 'ПІБ (лат.)' },
          { id: 'Стать', title: 'Стать' },
          { id: 'Громадянство', title: 'Громадянство' },
          { id: 'Серія, № паспорта', title: 'Серія, № паспорта' },
          { id: 'Дата народження', title: 'Дата народження' },
          { id: 'Назва приймаючої сторони', title: 'Назва приймаючої сторони' },
          {
            id: 'Адреса приймаючої сторони',
            title: 'Адреса приймаючої сторони',
          },
          { id: 'Діти', title: 'Діти' },
          { id: 'Ким введено інформацію', title: 'Ким введено інформацію' },
          { id: 'Phone', title: 'Phone' },
          { id: 'Address', title: 'Address' },
          { id: 'lat', title: 'Latitude' },
          { id: 'lon', title: 'Longitude' },
          { id: 'error', title: 'Error' },
        ],
        // fieldDelimiter: ';',
      });

      csvWriter.writeRecords(data).then(() => {
        console.log('...Done');
      });
    });
  });
