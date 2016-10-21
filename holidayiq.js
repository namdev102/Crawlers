 import request from 'request-promise';
import cheerio from 'cheerio';
import {merge, isUndefined} from 'lodash';
import moment from 'moment';

import {log} from 'app/util';
import {table} from 'app/orm';
import {errorLog} from 'app/util';
export default async function run(hotel, calledFromSyncTasks) {

  console.log('holidayIq Crawler is running...');

  const holidayIq_url = hotel.meta.holidayiq_url;
  // let $ = await fetchHtml(holidayIq_url);
  if (! hotel.meta.holidayiq_url || hotel.meta.holidayiq_url === 'na') {
    return;
  }

  log('crawler:holidayiq', 'info', `Crawling hotel url ${holidayIq_url}`);

  const userName = 'hiq';
  const password = '123';
  const options = {
    json: true,
    headers: {
      'Authorization': 'Basic ' + new Buffer(userName + ':' + password).toString('base64')
    }
  };

  // obtaining hoteliday iq ota
  const ota = await table('otas').find('slug', 'holidayiq');

  // fetching holiday iq hotel data
  // Breakage Possible *********************************************************
  let holidayIqData;
  try {
    holidayIqData = await agentRequest(holidayIq_url, options);
  } catch(err) {
    // Parmeters for function
    // ota_id, hotel_id, err, type [hotel_ota_credentials, url_error, global], place
    await errorLog(ota.id, hotel.id, err, 'url_error', 'Error while fetching JSON Data from api', {url: holidayIq_url});
  }

  // inserting holidayiq hotel metadata
  await table('crawler_hotel_metadata').insert({
    hotel_id: hotel.id,
    ota_id: ota.id,
    data: {data: holidayIqData}
  });

  let reviews;
  if(holidayIqData) {
    reviews = holidayIqData.latestReviews;
  }

  let newReviews;
  if(calledFromSyncTasks) {
    // extracting the last crawled data
    const lastCrawledReview = await table('crawler_reviews')
      .where({
        hotel_id: hotel.id,
        ota_id: ota.id
      })
      .orderBy('reviewed_on', 'desc')
    ;
    // sorting the reviews based on date
    let sortedByReviewDate;
    if(reviews) {
      sortedByReviewDate = reviews.sort((reviewA, reviewB) => {
        const timestampA = parseInt(moment(reviewA.reviewDate).format('x'), 10);
        const timestampB = parseInt(moment(reviewB.reviewDate).format('x'), 10);
        return timestampB-timestampA;
      });
    }

    log('crawler:holidayiq', 'info', `Total reviews ${sortedByReviewDate.length}`);
    if(lastCrawledReview && sortedByReviewDate) {
      newReviews = filterReviews(sortedByReviewDate.filter(({reviewDate}) => {
        return (moment(reviewDate).toDate() >= moment().subtract(5, 'months').startOf('day').toDate()) &&
        (moment(reviewDate).toDate() <= moment().startOf('day').toDate());}), lastCrawledReview);
    } else {
      newReviews = sortedByReviewDate;
    }
    let alreadyInDb;
    try {
      alreadyInDb = await table('crawler_reviews')
        .select('uuid_on_ota')
        .where({
          ota_id: ota.id,
          hotel_id: hotel.id
        })
        .whereIn('uuid_on_ota', reviews.map((r) => r.id))
        .all()
      ;
    } catch (err) {
      console.log(err);
    }

    log('crawler:holidayiq', 'info', `Total reviews in database ${alreadyInDb.length}`);
    try {
      newReviews = newReviews.filter((r) => {
        return alreadyInDb.map((r) => r.uuid_on_ota).indexOf(`${r.id}`) < 0;
      });
    } catch(err) {
      // Parmeters for function
      // ota_id, hotel_id, err, type [hotel_ota_credentials, url_error, global], place
      await errorLog(ota.id, hotel.id, err, 'new_error', 'Error while Filtering New Reviews with calledFromSyncTasks');
    }

  } else {
    // filter out reviewIds which are already in db
    let alreadyInDb;
    try {
      alreadyInDb = await table('crawler_reviews')
        .select('uuid_on_ota')
        .where({
          ota_id: ota.id,
          hotel_id: hotel.id
        })
        .whereIn('uuid_on_ota', reviews.map((r) => r.id))
        .all()
      ;
    } catch (err) {
      console.log(err);
    }
    if(alreadyInDb) {
      log('crawler:holidayiq', 'info', `Total reviews in database ${alreadyInDb.length}`);
    }
    try {
      newReviews = reviews.filter((r) => {
        return alreadyInDb.map((r) => r.uuid_on_ota).indexOf(`${r.id}`) < 0;
      });
    } catch(err) {
      // Parmeters for function
      // ota_id, hotel_id, err, type [hotel_ota_credentials, url_error, global], place
      await errorLog(ota.id, hotel.id, err, 'new_error', 'Error while Filtering New Reviews without calledFromSyncTasks');
    }
  }
  if(newReviews) {
    log('crawler:holidayiq', 'info', `Total new reviews ${newReviews.length}`);
  }
  //  inserting new crawled reviews
  try {
    await table('crawler_reviews').insert(
      newReviews.map((review) => {
        return {
          hotel_id: hotel.id,
          ota_id: ota.id,
          uuid_on_ota: review.id,
          meta: review,
          reviewed_on: new Date(review.reviewDate)
        };
      })
    );
  } catch(err) {
    await errorLog(ota.id, hotel.id, err, 'global', 'DB Error while inserting into database');
  }
}

export async function fetchHtml(url, options) {

  const bodyHtml = await agentRequest(url, options);
  // console.log(cheerio.load(bodyHtml));
  return cheerio.load(bodyHtml);
}

export async function agentRequest(url, options) {

  return await request(merge({
    uri: url,
    headers: {'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; r+v:36.0) Gecko/20100101 Firefox/36.0'
    }
  }, options));
}

export function filterReviews(currentDateReviews, lastCrawledReview) {
  let index = 0; let matched = false;
  currentDateReviews.map(({id, reviewDate}, key) => {
    if(id === lastCrawledReview.uuid_on_ota) {
      matched = true;
      index = key;
    }
  });
  const newReviews = matched ? currentDateReviews.slice(0, index) : currentDateReviews;
  return newReviews;
}
