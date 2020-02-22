const fs = require('fs');
const csv = require('csv-parser');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const axios = require('axios');
const https = require('https');
const resultsRead = [];

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

function geocoding(value) {
  return axios.get('https://api', {
    params: {
      text: value,
      limit: 1,
    },
    httpsAgent: new https.Agent({
      rejectUnauthorized: false,
    }),
    timeout: 60000,
  });
}

fs.createReadStream('false-2-ok.csv')
  .pipe(csv())
  .on('data', data => resultsRead.push(data))
  .on('end', () => {
    const getData = () =>
      Promise.all(
        resultsRead.map(async (item, index) => {
          await sleep(index * 1000);

          try {
            let lat = null;
            let lng = null;

            const response = await geocoding(item['Full Address']);

            const { geo_centroid } = response.data || {};
            console.log(geo_centroid);
            if (geo_centroid) {
              lat = geo_centroid.coordinates[1];
              lng = geo_centroid.coordinates[0];
            }

            return {
              ...item,
              lat,
              lng,
            };
          } catch (error) {
            console.log(error.message);
            return { ...item, error: error.message };
          }
        })
      );

    getData().then(data => {
      console.log(data);

      const csvWriter = createCsvWriter({
        path: 'file.csv',
        header: [
          { id: '№ объекта', title: '№ объекта' },
          { id: 'Адрес объекта', title: 'Адрес объекта' },
          { id: 'Город', title: 'Город' },
          { id: 'Статус', title: 'Статус' },
          { id: 'Region', title: 'Region' },
          { id: 'Locality', title: 'Locality' },
          { id: 'Address', title: 'Address' },
          { id: 'Full Address', title: 'Full Address' },
          { id: 'Latitude', title: 'Latitude' },
          { id: 'Longitude', title: 'Longitude' },
          { id: 'lat', title: 'Lat' },
          { id: 'lng', title: 'Lng' },
          { id: 'error', title: 'Error' },
        ],
      });

      csvWriter.writeRecords(data).then(() => {
        console.log('...Done');
      });
    });
  });
