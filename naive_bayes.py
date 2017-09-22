import re
import time
#from stop_words import get_stop_words
#stop_words = get_stop_words('english')
import nltk
import string
import math
nltk.download('stopwords')
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer
from nltk import word_tokenize

#################################################################################
########################### GLOBAL VARIABLES ####################################
#################################################################################
dictLabelsDocCount = dict()
dictLabelsWordsCount = dict()
dictWords = dict()
#totaldoc = 0

def mainFunc():
	traintime1 = time.clock()
	trainNaiveBayes("DBPedia.full/full_train.txt")
	traintime2 = time.clock()
	##print(dictLabelsDocCount.values())
	#print(sum(dictLabelsDocCount.values()))
	#print(len(dictLabelsDocCount.keys()))
	#print(dictLabelsDocCount.keys())
	#print(dictLabelsWordsCount)
	params = 0
	params = params + len(dictLabelsDocCount.keys())
	for val in dictLabelsWordsCount:
		params = params + len(dictLabelsWordsCount.keys())
	testtime1 = time.clock()
	accuracyTrain = testNaiveBayes("DBPedia.full/full_train.txt")
	testtime2 = time.clock()
	accuracyTest = testNaiveBayes("DBPedia.full/full_test.txt")
	testtime3 = time.clock()
	print("Train Accuracy = "+str(accuracyTrain))
	print("Test Accuracy = "+str(accuracyTest))
	print("Number of parameters = "+ params)
	print("Train time = "+(traintime2-traintime1))
	print("Training set test time = "+ (testtime2-testtime1))
	print("Test set test time = "+ (testtime3-testtime2))
	
	
def trainNaiveBayes(filename):
	train = open(filename, "r")
	labels = []
	doc = []
	text = []
	i = 1
	j=0
	for line in train:
		if(i>3):
			#totaldoc = totaldoc + 1
			document = line.split("\t")
			labels = document[0].strip().split(",")
			document[1] = document[1].replace("\\\"", " ")
			textTemp = document[1].split("\"")
			textTokens = preprocessText(textTemp[1])
			#print(textTokens)
			for label in labels:
				#print("inner loop train")
				###################################################################
				if(label in dictLabelsDocCount.keys()):
					dictLabelsDocCount[label] = dictLabelsDocCount[label]+1
				else:
					dictLabelsDocCount[label] = 1
					#print(label)
					j=j+1
				###################################################################
				
				###################################################################
				if(label in dictLabelsWordsCount.keys()):
					for token in textTokens:
						if(token in dictLabelsWordsCount[label].keys()):
							dictLabelsWordsCount[label][token] = dictLabelsWordsCount[label][token]+1
						else:
							dictLabelsWordsCount[label][token] = 1
						if(token not in dictWords):
							dictWords[token] = 1
				else:
					dictLabelsWordsCount[label] = dict()
					for token in textTokens:
						dictLabelsWordsCount[label][token] = 1
						if(token not in dictWords):
							dictWords[token] = 1
				###################################################################
		i=i+1
		#print(i)
	#print(j)
	return
		
def testNaiveBayes(filename):
	train = open(filename, "r")
	labels = []
	doc = []
	text = []
	i = 1
	totaldoc = sum(dictLabelsDocCount.values())
	#print("total doc = "+str(totaldoc))
	totalTestDoc = 0
	accuracy = 0
	predClass = ""
	totLabels = len(dictLabelsDocCount.keys())
	dictwordcnt = float(1/len(dictWords.keys()))
	for line in train:
		if(i>3):
			probabilityPred = 100000000
			document = line.split("\t")
			labels = document[0].strip().split(",")
			document[1] = document[1].replace("\\\"", " ")
			textTemp = document[1].split("\"")
			textTokens = preprocessText(textTemp[1])
			for label in dictLabelsDocCount.keys():
				#print("inner loop test")
				countWord = sum(dictLabelsWordsCount[label].values())
				probability = float(math.log((dictLabelsDocCount[label] + float(1/totLabels)) /float(totaldoc + 1)))
				#print("probability y = "+str(probability))
				for token in textTokens:
					if(token in dictLabelsWordsCount[label].keys()):
						countToken = dictLabelsWordsCount[label][token]
					else:
						countToken = 0
					#probability = probability + float(math.log((countToken + float(1/len(dictWords.keys())))/float(countWord + 1 )))
					probability = probability + float(math.log((countToken + dictwordcnt)/float(countWord + 1 )))
				probability = probability*(-1)
				if(probability < probabilityPred):
					probabilityPred = probability
					predClass = label
				#print("probability = "+str(probability))
			if(predClass in labels):
				accuracy = accuracy + 1
			#print(predClass,labels)
			totalTestDoc = totalTestDoc + 1
		i=i+1
		print(i)
	return(accuracy/totalTestDoc)

def preprocessText(text):
	#text = text.lower().translate(None, string.punctuation)
	#print("processing text...")
	text = text.translate(str.maketrans('','',string.punctuation))
	tokens = tokenize(text)
	return(tokens)
	

def tokenize(text):
	tokens = word_tokenize(text)
	stems = []
	for item in tokens:
		#stemTemp = PorterStemmer().stem(item)
		if(item not in stopwords.words('english')):
			stems.append(item.strip())
	#vocab = sorted(set(words))
	#return(lemmatizeTokens(stems))
	return(stems)

def lemmatizeTokens(tokens):
	wnl = nltk.WordNetLemmatizer()
	lemmtokens = [wnl.lemmatize(t) for t in tokens]
	return(lemmtokens)
	
	
mainFunc()