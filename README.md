# GCP Pub/Sub Twitter 

Publishes and consumes tweets to/from GCP Pub/Sub.

## Configuration

The following environment variable must be set to use both Publisher and Subscriber:
- GOOGLE_APPLICATION_CREDENTIALS

The following system properties must be set to produce data:
- Twitter client (You can obtain their values at the [Twitter Developer site](https://developer.twitter.com/en.html))
    - API_KEY
    - API_SECRET_KEY
    - ACCESS_TOKEN
    - ACCESS_TOKEN_SECRET
- App publisher
    - PROJECT_ID
    - TOPIC_ID
    - TWITTER_TERMS

The following system properties must be set to consume data:
- App subscriber
    - PROJECT_ID
    - SUBSCRIPTION_ID

To create the topics and subscriptions, use:

```shell script
$ gcloud pubsub topics create twitter-in
$ gcloud pubsub subscriptions create sub-twitter-in --topic twitter-in
```
