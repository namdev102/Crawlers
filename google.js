import request from 'request-promise';
import {merge} from 'lodash';
import moment from 'moment';

import {log} from 'app/util';
import {table} from 'app/orm';

export default async function run(hotel, calledFromSyncTasks) {

  console.log('googleReviews Crawler is running...');

  // const google_url = hotel.meta.google_url;
  // const google_url = 'https://maps.googleapis.com/maps/api/place/details/json?placeid=ChIJ-QhVW2kcDTkRLPJqg8lkiYo&key=AIzaSyBC7YT02fQ_8hVMlT9jr5znknrdXbjE0lg';
  const google_url = 'https://maps.googleapis.com/maps/api/place/nearbysearch/json?location=-33.8670522,151.1957362&radius=500&type=restaurant&name=cruise&key=AIzaSyBC7YT02fQ_8hVMlT9jr5znknrdXbjE0lg';

  // if (! hotel.meta.google_url || hotel.meta.google_url === 'na') {
  //   return;
  // }

  log('crawler:google', 'info', `Crawling hotel url ${google_url}`);

  // obtaining hoteliday iq ota
  const ota = await table('otas').find('slug', 'google');

  // fetching holiday iq hotel data
  const googleData = await agentRequest(google_url);
  const googleJsonData = JSON.parse(googleData);
  // console.log(googleJsonData.result.reviews);

  // inserting holidayiq hotel metadata
  await table('crawler_hotel_metadata').insert({
    hotel_id: hotel.id,
    ota_id: ota.id,
    data: {data: googleJsonData}
  });

  const reviews = googleJsonData.result.reviews;
  // inserting reviews
  await * reviews.map(async (review) => {
    let reviewContent = '';
    reviewContent = `${reviewContent}${review.time}`;
    reviewContent = `${reviewContent}${review.author_url}`;
    reviewContent = `${reviewContent}${review.text}`;

    const uuidData = new Buffer(reviewContent).toString('base64');
    let uuid;
    if(uuidData.length >= 254) {
      uuid = uuidData.slice(0, 250);
    } else {
      uuid = uuidData;
    }
    review["id"] = uuid;
    const existing = await table('crawler_reviews').find({
      hotel_id: hotel.id,
      ota_id: ota.id,
      uuid_on_ota: review.id
    });
    if(!existing) {
      await table('crawler_reviews').insert({
        hotel_id: hotel.id,
        ota_id: ota.id,
        uuid_on_ota: review.id,
        meta: review,
        reviewed_on: convertTimestamp(review.time)
      });
      log('crawler:google', 'info', 'inserting review into database');
    } else {
      log('crawler:google', 'info', 'review already exists');
    }
  });

  let newReviews;
  if(calledFromSyncTasks) {
    // extracting the last crawled data
    const lastCrawledReview = await table('crawler_reviews')
      .where({
        hotel_id: hotel.id,
        ota_id: ota.id
      })
      .orderBy('reviewed_on', 'desc')
      .limit(1)
      .find()
    ;
    // sorting the reviews based on date
    const sortedByReviewDate = reviews.sort((reviewA, reviewB) => {
      const timestampA = parseInt(moment(convertTimestamp(reviewA.time)).format('x'), 10);
      const timestampB = parseInt(moment(convertTimestamp(reviewB.time)).format('x'), 10);
      return timestampB-timestampA;
    });

    log('crawler:google', 'info', `Total reviews ${sortedByReviewDate.length}`);
    if(lastCrawledReview) {
      newReviews = filterReviews(sortedByReviewDate.filter(({reviewDate}) => {
        return (moment(reviewDate).toDate() >= moment().subtract(2, 'days').startOf('day').toDate()) &&
        (moment(reviewDate).toDate() <= moment().startOf('day').toDate());}), lastCrawledReview);
    } else {
      newReviews = sortedByReviewDate;
    }
    // log('crawler:goibio', 'info', `Total new reviews ${newReviews.length}`);

  } else {
    // filter out reviewIds which are already in db
    const alreadyInDb = await table('crawler_reviews')
      .select('uuid_on_ota')
      .where({
        ota_id: ota.id,
        hotel_id: hotel.id
      })
      .whereIn('uuid_on_ota', reviews.map((r) => r.id))
      .all()
    ;

    log('crawler:google', 'info', `Total reviews in database ${alreadyInDb.length}`);

    // need to correct this ...see from holidayiq
    newReviews = reviews.filter((r) => {
      return alreadyInDb.map((r) => r.uuid_on_ota).indexOf(r.id) < 0;
    });

    log('crawler:google', 'info', `Total new reviews ${newReviews.length}`);
  }
  //  inserting new crawled reviews
  await table('crawler_reviews').insert(
    newReviews.map((review) => {
      return {
        hotel_id: hotel.id,
        ota_id: ota.id,
        uuid_on_ota: review.id,
        meta: review,
        reviewed_on: convertTimestamp(review.time)
      };
    })
  );
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

function convertTimestamp(timestamp) {
  const d = new Date(timestamp * 1000);	// Convert the passed timestamp to milliseconds
  const yyyy = d.getFullYear();
  const mm = ('0' + (d.getMonth() + 1)).slice(-2);	// Months are zero based. Add leading 0.
  const dd = ('0' + d.getDate()).slice(-2);			// Add leading 0.
  const hh = d.getHours();
  let h = hh;
  const min = ('0' + d.getMinutes()).slice(-2);	// Add leading 0.
  let ampm = 'AM';

  if (hh > 12) {
    h = hh - 12;
    ampm = 'PM';
  } else if (hh === 12) {
    h = 12;
    ampm = 'PM';
  } else if (hh === 0) {
    h = 12;
  }

	// ie: 2013-02-18, 8:35 AM
  const time = yyyy + '-' + mm + '-' + dd + ', ' + h + ':' + min + ' ' + ampm;

  return time;
}
