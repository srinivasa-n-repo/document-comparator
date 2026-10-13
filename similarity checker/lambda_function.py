import json
import logging
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def calculate_cosine_similarity(doc1, doc2):
    tfidf_vectorizer = TfidfVectorizer()
    tfidf_matrix = tfidf_vectorizer.fit_transform([doc1, doc2])
    # print('\ndoc1:',doc1, '\ndoc2:', doc2, '\nsimilarity:', cosine_similarity(tfidf_matrix[0:1], tfidf_matrix[1:2])[0][0])
    return cosine_similarity(tfidf_matrix[0:1], tfidf_matrix[1:2])[0][0]


def lambda_handler(event, context):

    try:
        if event['event_type'] == 'check_similarity':
            minimum_similarity_score = 1.0
            load_hand, load_digital = event['documents']

            for i in range(max(len(load_digital), len(load_hand))):
                document1 = ""
                document2 = ""
                try:
                    document1 = load_hand[i]
                except (IndexError, TypeError):
                    document1 = ""
                    logger.warning("Hand scanned document missing at index %s", i)
                try:
                    document2 = load_digital[i]
                    if document2 == '-----':
                        document2 = ""
                except (IndexError, TypeError):
                    document2 = ""
                    logger.warning("Digital document missing at index %s", i)

                logger.debug("Page %s — doc1 length: %s, doc2 length: %s", i, len(document1), len(document2))

                if len(document2) - len(document1) > 5000:
                    similarity_score = 0
                elif len(document1) == 0 and len(document2) == 0:
                    logger.info("Both documents empty at index %s", i)
                    similarity_score = 0
                else:
                    similarity_score = calculate_cosine_similarity(document1, document2)

                logger.info("Page %s cosine similarity: %s", i, similarity_score)
                minimum_similarity_score = min(similarity_score, minimum_similarity_score)

            logger.info("Minimum similarity score: %s", minimum_similarity_score)
            return {
                'body': json.dumps({"status": minimum_similarity_score})
            }
    except Exception as e:
        return {
            'body': json.dumps({"status": 0})
        }