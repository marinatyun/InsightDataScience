"""
Insight Data Ebgineering Code Challenge - Digital Wallet
"""

import argparse

class PayMo:
	def __init__(self, batchPayment):
		"""
		Initialize social network using transcation records in bacthPayment file.
		The social network is stored in a distionary:
		  - key : Id of the user
			- value : a set of users that have transaction with the 'key' user before
		"""
		self.network = {}
		with open(batchPayment, encoding='utf-8', newline='\n') as fin:
			# skip the header line
			fin.readline()
			for line in fin:
				tokens = line.split(',')
				id1 = tokens[1].strip()
				id2 = tokens[2].strip()
				self.__addPayment(id1, id2)
	
	def __addPayment(self, id1, id2):
		"""Add a payment into the social network"""
		self.network.setdefault(id1, set()).add(id2)
		self.network.setdefault(id2, set()).add(id1)
	
	def ProcessStreamPayment(self, streamPayment, output1, output2, output3):
		"""
		Process payments line by line
		  - streamPayment : input payment files
			- output1 : results from feature1 will be written in this file
			- output2 : results from feature2 will be written in this file
			- output3 : results from feature3 will be written in this file
		"""
		with open(streamPayment, encoding='utf-8', newline='\n') as fin, \
				open(output1, 'w') as fout1, \
				open(output2, 'w') as fout2, \
				open(output3, 'w') as fout3:
			# skip the header line
			fin.readline()
			count = 0
			for line in fin:
				tokens = line.split(',')
				id1 = tokens[1].strip()
				id2 = tokens[2].strip()
				self.__writeMessage(id1, id2, 1, fout1)
				self.__writeMessage(id1, id2, 2, fout2)
				self.__writeMessage(id1, id2, 4, fout3)
				self.__addPayment(id1, id2)
				count = count + 1
				if count % 10000 == 0:
					print("Processed {} payments".format(count))
	
	def __writeMessage(self, id1, id2, maxDepth, fout):
		"""Write message into file fout"""
		if self.__isFriend(id1, id2, maxDepth):
			fout.write('trusted\n')
		else:
			fout.write('unverified\n')

	def __isFriend(self, id1, id2, depth):
	    """
		Return true if id2 are in id1's maxDepth degree friends network.
		  - id1 : Id of the first user
			- id2 : Id of the second user
			- maxDepth : = 1 for feature 1, = 2 for feature 2, = 4 for feature 4
		"""
		if id1 not in self.network or id2 not in self.network:
			return False
		depth1 = int(depth/2)
		depth2 = depth - depth1
		network1, find = self.__expandNetwork(id1, id2, depth1)
		if find:
			return True
		network2, find = self.__expandNetwork(id2, id1, depth2)
		if find:
			return True
		return len(network1.intersection(network2)) != 0

	def __expandNetwork(self, id1, id2, maxDepth):
		"""
		Expand the social network for id1 upto maxDepth. During the expansion process, if id2
		is found in the network, the expansion will be terminated early, and return a tuple
		of (partialNetwork, True). If id2 is not found, return (fullNetwork, False).
		"""
		id1Network = set()
		id1Network.add(id1)
		depth = 0
		while depth < maxDepth:
			oldNetwork = id1Network.copy()
			for key in oldNetwork:
				if id2 in self.network[key]:
					return (id1Network, True)
				id1Network.update(self.network[key])
			depth = depth + 1
		return (id1Network, False)


def main():
	parser = argparse.ArgumentParser(description=__doc__)
	parser.add_argument('batchPayment', type=str, help='path to the batch_payment file')
	parser.add_argument('streamPayment', type=str, help='path to the stream_payment file')
	parser.add_argument('output1', type=str, help='path to the output file for feature1')
	parser.add_argument('output2', type=str, help='path to the output file for feature2')
	parser.add_argument('output3', type=str, help='path to the output file for feature3')
	args = parser.parse_args()

	print("Initialize social network")
	wallet = PayMo(args.batchPayment)
	print("Processing stream payment")
	wallet.ProcessStreamPayment(args.streamPayment, args.output1, args.output2, args.output3)

if __name__ == '__main__':
	main()
