import nltk
import string
from nltk.tokenize import TweetTokenizer

punctuations = list(string.punctuation)
stopwords = nltk.corpus.stopwords.words('english')
tknzr = TweetTokenizer()
porter = nltk.PorterStemmer()

text_list = tknzr.tokenize("All the food is great here. But the best thing they have is their wings. Their wings are simply fantastic!!  The \"Wet Cajun\" are by the best & most popular.  I also like the seasoned salt wings.  Wing Night is Monday & Wednesday night, $0.75 whole wings!\n\nThe dining area is nice. Very family friendly! The bar is very nice is well.  This place is truly a Yinzer's dream!!  \"Pittsburgh Dad\" would love this place n'at!!")

filtered_word = [w for w in filtered_words if (w.lower() not in stopwords and w.lower() not in punctuations)]
stemmed_word =[porter.stem(t) for t in filtered_word]
nltk.pos_tag(stemmed_word)
