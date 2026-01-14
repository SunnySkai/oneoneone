import logger from '#modules/logger/index.js';
import config from '#config';
import retryable from '#modules/retryable/index.js';
import retryFetch from '#modules/retry-fetch/index.js';

/**
 * Parse Gravity API response to extract required tweet fields
 * Filters out tweets that don't have all required fields
 * @param {Array} data - The data array from Gravity API response
 * @returns {Array} Array of parsed and validated tweet objects
 */
function parseGravityResponse(data) {
  const parsedTweets = [];

  for (const item of data) {
    // Check if all required fields are present and valid
    if (!item.tweet?.id ||
        !item.user?.username ||
        !item.text ||
        !item.datetime ||
        !item.uri ||
        !item.user?.id ||
        !item.user?.display_name ||
        typeof item.user?.followers_count !== 'number' ||
        typeof item.user?.following_count !== 'number' ||
        typeof item.user?.verified !== 'boolean'
      ) {
      // Skip this tweet if any required field is missing or invalid
      continue;
    }

    const tweet = {
      // Required fields for validation
      tweetId: item.tweet.id,
      username: item.user.username,
      text: item.text,
      createdAt: item.datetime,
      tweetUrl: item.uri,
      hashtags: item.tweet.hashtags || [],

      // Additional required fields
      userId: item.user.id,
      displayName: item.user.display_name,
      followersCount: item.user.followers_count,
      followingCount: item.user.following_count,
      verified: item.user.verified
    };

    // Only include userDescription if it's not null
    if (item.user.user_description !== null && item.user.user_description !== undefined) {
      tweet.userDescription = item.user.user_description;
    }

    parsedTweets.push(tweet);
  }

  return parsedTweets;
}

/**
 * Fetches X/Twitter tweets for a given keyword using the Gravity API from Macrocosmos.
 *
 * This function retrieves tweets for a specific keyword.
 * It uses the configured Gravity API to fetch tweets with retry logic for reliability.
 * The number of tweets fetched is determined by the GRAVITY_TWEET_LIMIT environment variable.
 *
 * @example
 * ```javascript
 * const tweets = await fetch({
 *   keyword: 'bitcoin'
 * });
 * ```
 *
 * @param {Object} parameters - The parameters for fetching tweets
 * @param {string} parameters.keyword - The keyword to search for tweets
 * @returns {Promise<Array>} A promise that resolves to an array of tweet objects
 * @throws {Error} Throws an error if the Gravity API fails or if there are network issues
 *
 * @description
 * - Uses retryable wrapper with up to 10 retry attempts for reliability
 * - Logs detailed information about the fetch process
 * - Tweet count is configured via GRAVITY_TWEET_LIMIT environment variable
 * - Utilizes the configured Gravity API from `config.MINER.X_TWEETS.GRAVITY_API_URL`
 */

const sleep = (ms) => {
  return new Promise(resolve => setTimeout(resolve, ms));
}

const convertTweet = (tweet) => {
  const author = tweet.author || {};
  const entities = tweet.entities || {};
  const hashtags = (entities.hashtags || []).map(h => `#${h.text || h}`);

  return {
    user: {
      user_blue_verified: author.isBlueVerified || false,
      user_description: author.description || '',
      profile_image_url: author.profilePicture || '',
      followers_count: author.followers || 0,
      following_count: author.following || 0,
      cover_picture_url: author.coverPicture || '',
      display_name: author.name || '',
      user_location: author.location || '',
      verified: author.isVerified || false,
      id: author.id || '',
      username: author.userName || ''
    },
    source: 'x',
    datetime: tweet.createdAt ? new Date(tweet.createdAt).toISOString() : '',
    uri: tweet.url || '',
    text: tweet.text || '',
    tweet: {
      view_count: tweet.viewCount || 0,
      bookmark_count: tweet.bookmarkCount || 0,
      id: tweet.id || '',
      is_quote: !!tweet.quoted_tweet,
      like_count: tweet.likeCount || 0,
      is_reply: tweet.isReply || false,
      conversation_id: tweet.conversationId || '',
      hashtags: hashtags,
      language: tweet.lang || '',
      quote_count: tweet.quoteCount || 0,
      retweet_count: tweet.retweetCount || 0,
      reply_count: tweet.replyCount || 0
    }
  };
}

const fetchPaginationTweets = async (keyword, maxDuration = 100) => {
  const allTweets = [];
  let cursor = '';
  const startTime = Date.now();

  while (true) {
    const elapsedTime = (Date.now() - startTime) / 1000;
    if (elapsedTime >= maxDuration) {
      console.log(`Time limit reached (${elapsedTime.toFixed(2)}s). Stopping pagination.`);
      break;
    }

    console.log(`Fetching page with cursor: '${cursor}'...`);

    try {
      const url = new URL(config.MINER.X_TWEETS.TWITTER_API_URL);
      url.searchParams.append('query', keyword);
      url.searchParams.append('queryType', config.MINER.X_TWEETS.TWITTER_QUERY_TYPE);
      url.searchParams.append('cursor', cursor);

      const response = await fetch(url.toString(), {
        method: 'GET',
        headers: {
          'X-API-Key': process.env.TWITTER_API_TOKEN
        }
      });

      if (!response.ok) {
        if (response.status === 429) {
          console.log('Rate limit hit (429). Waiting 5 seconds before retry...');
          await sleep(5000);
          continue;
        }
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      const data = await response.json();
      const tweets = data.tweets || [];
      
      // Convert and add tweets
      const convertedTweets = tweets.map(tweet => convertTweet(tweet));
      allTweets.push(...convertedTweets);
      
      console.log(`Fetched ${tweets.length} tweets. Total: ${allTweets.length}`);

      // Check if there's a next page
      const hasNextPage = data.has_next_page || false;
      const nextCursor = data.next_cursor || '';

      if (!hasNextPage || !nextCursor) {
        console.log('No more pages available.');
        break;
      }

      cursor = nextCursor;

      // Wait 5 seconds before next request
      console.log('Waiting 5 seconds before next request...');
      await sleep(5000);

    } catch (error) {
      console.error('Error fetching data:', error.message);
      break;
    }
  }

  return allTweets;
}

const fetchTweets = async ({ keyword }) => {
  try {
    // Check for required environment variables
    if (!process.env.TWITTER_API_TOKEN) {
      throw new Error('TWITTER_API_TOKEN not configured');
    }
    
    if (!process.env.TWITTER_MAX_DURATION) {
      throw new Error('TWITTER_MAX_DURATION not configured');
    }

    const cleanKeyword = keyword.replaceAll(/^"|"$/g, '');

    logger.info(`[Miner] Fetching tweets - Keyword: ${keyword} , Duration: ${process.env.TWITTER_MAX_DURATION}`);

    // Run the Gravity API and get the results
    logger.info(`[Miner] Starting Gravity API for tweets fetch...`);
    const items = await fetchPaginationTweets(cleanKeyword, process.env.TWITTER_MAX_DURATION);

    // Return structured response with tweets
    return items;
  } catch (error) {
    logger.error(`[Miner] Error fetching tweets:`, error);
    throw error;
  }
}

export default fetchTweets

