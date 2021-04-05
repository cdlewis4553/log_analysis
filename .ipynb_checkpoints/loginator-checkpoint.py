import functools
import re

FILE_NAME = ""
NAMED_COLUMNS = True
ROW_DELIMITER = "\n"
COLUMN_DELIMITER = " "

# Information on the compareStrings function and the sortHack global it uses are detailed below
#
# My sorting method: https://stackoverflow.com/questions/42899405/sort-a-list-with-longest-items-first
# My problem is different than the OP's, but similar in the fact that I want to deviate from the normal
# lexicographical order used Python for string sorting.
# These are the three rules I settled on:
# - First, I want all numerics to be in numeric order as opposed to lexicographical order.
# - Second, I want all non-numerics to be in normal lexicographical order.
# - Finally, I want all numerics to come before any non-numerics.
# These three conditions are done by the compareStrings function.
# The compareStrings function is passed as a parameter to functools's cmp_to_key function.
# However, there is a problem with this.
# The index of the column being sorted by isn't going to be known until runtime.
# But I can't pass the index into the compareStrings function because the cmp_to_key function is expecting the
# function passed into it to only have two parameters.
# By making it global the compareStrings function can always see it and doesn't need it passed as a parameter.
#
# If anyone knows a better way, let me know.

def compareStrings(a, b):
	a = a[sortHack]
	b = b[sortHack]
	aFloat = False
	bFloat = False
	try:
		a = float(a)
		aFloat = True
	except ValueError:
		pass
	try:
		b = float(b)
		bFloat = True
	except ValueError:
		pass
	if aFloat and not bFloat:
		return -1
	if not aFloat and bFloat:
		return 1
	if a < b:
		return -1
	if a > b:
		return 1
	return 0

class DataFrame:
	def __init__(self, rawData):
		self.numberColumns = len(rawData[0])
		if NAMED_COLUMNS:
			self.columnNames = rawData.pop(0)
		else:
			self.columnNames = []
			for i in range(self.numberColumns):
				self.columnNames.append("Column " + str(i))
		self.rawData = rawData
		self.numberRows = len(rawData)
		self.maxLength = []
		self.frequencyValues = []
		self.frequencyCount = []
		self.numericValues = []
		self.numericCount = []
		self.meanValue = []
		self.medianValue = []
		self.modeValue = []
		self.modeCount = []
		self.minValue = []
		self.maxValue = []
		self.rangeValue = []
		self.sumValue = []
		self.formatString = "{:<" + str(max(len(str(self.numberRows - 1)) + 4, 15))
		for i in range(self.numberColumns):
			self.maxLength.append(len(self.columnNames[i]))
			self.frequencyValues.append([])
			frequencyValues = []
			self.numericValues.append([])
			for line in self.rawData:
				frequencyValues.append(line[i])
				self.maxLength[i] = max(self.maxLength[i], len(line[i]))
				try:
					self.numericValues[i].append(float(line[i]))
				except ValueError:
					pass
			frequencyValues.sort()
			currentCount = 0
			currentElement = frequencyValues[0]
			for item in frequencyValues:
				if item != currentElement:
					self.frequencyValues[i].append([currentElement, currentCount])
					currentCount = 0
					currentElement = item
				currentCount = currentCount + 1
			self.frequencyValues[i].append([currentElement, currentCount])
			self.frequencyValues[i].sort(reverse = True, key=lambda x: x[1])
			if len(self.numericValues[i]) > 0:
				self.numericValues[i].sort()
				self.meanValue.append(sum(self.numericValues[i]) / len(self.numericValues[i]))
				self.medianValue.append(self.numericValues[i][len(self.numericValues[i]) // 2])
				self.modeValue.append(0)
				self.modeCount.append(0)
				currentCount = 0
				currentElement = self.numericValues[i][0]
				for item in self.numericValues[i]:
					if item != currentElement:
						if currentCount > self.modeCount[i]:
							self.modeValue[i] = currentElement
							self.modeCount[i] = currentCount
						currentCount = 0
						currentElement = item
					currentCount = currentCount + 1
				if currentCount > self.modeCount[i]:
					self.modeValue[i] = currentElement
					self.modeCount[i] = currentCount
				self.minValue.append(self.numericValues[i][0])
				self.maxValue.append(self.numericValues[i][-1])
				self.rangeValue.append(self.maxValue[i] - self.minValue[i])
				self.sumValue.append(sum(self.numericValues[i]))
			else:
				self.meanValue.append("-")
				self.medianValue.append("-")
				self.modeValue.append("-")
				self.modeCount.append("-")
				self.minValue.append("-")
				self.maxValue.append("-")
				self.rangeValue.append("-")
				self.sumValue.append("-")
			self.frequencyCount.append(len(self.frequencyValues[i]))
			self.maxLength[i] = max(self.maxLength[i], len(str(self.frequencyCount[i])))
			self.numericCount.append(len(self.numericValues[i]))
			self.maxLength[i] = max(self.maxLength[i], len(str(self.numericCount[i])))
			self.maxLength[i] = max(self.maxLength[i], len(str(self.meanValue[i])))
			self.maxLength[i] = max(self.maxLength[i], len(str(self.medianValue[i])))
			self.maxLength[i] = max(self.maxLength[i], len(str(self.modeValue[i])))
			self.maxLength[i] = max(self.maxLength[i], len(str(self.modeCount[i])))
			self.maxLength[i] = max(self.maxLength[i], len(str(self.minValue[i])))
			self.maxLength[i] = max(self.maxLength[i], len(str(self.maxValue[i])))
			self.maxLength[i] = max(self.maxLength[i], len(str(self.rangeValue[i])))
			self.maxLength[i] = max(self.maxLength[i], len(str(self.sumValue[i])))
			self.formatString = self.formatString + "} {:<" + str(self.maxLength[i])
		self.formatString = self.formatString + "}"
		self.shortFormatString = self.formatString.split(" ")
		self.shortFormatString = self.shortFormatString[0] + " {:<}"

	def completePrint(self):
		print(self.shortFormatString.format("Number Columns:", str(self.numberColumns)))
		print(self.shortFormatString.format("Number Rows:", str(self.numberRows)))
		print(self.formatString.format("Column Name", *self.columnNames))
		print(self.formatString.format("Unique Values", *self.frequencyCount))
		print(self.formatString.format("Numeric Values", *self.numericCount))
		print(self.formatString.format("Mean Value", *self.meanValue))
		print(self.formatString.format("Median Value", *self.medianValue))
		print(self.formatString.format("Mode Value", *self.modeValue))
		print(self.formatString.format("Mode Count", *self.modeCount))
		print(self.formatString.format("Min Value", *self.minValue))
		print(self.formatString.format("Max Value", *self.maxValue))
		print(self.formatString.format("Range Value", *self.rangeValue))
		print(self.formatString.format("Sum Value", *self.sumValue))
		for i in range(self.numberRows):
			print(self.formatString.format("Row " + str(i), *self.rawData[i]))

	def rawPrint(self):
		if NAMED_COLUMNS:
			rawString = self.columnNames[0]
			for i in range(1, self.numberColumns):
				rawString = rawString + COLUMN_DELIMITER + self.columnNames[i]
			print(rawString, end=ROW_DELIMITER)
		for i in range(self.numberRows):
			rawString = self.rawData[i][0]
			for j in range(1, self.numberColumns):
				rawString = rawString + COLUMN_DELIMITER + self.rawData[i][j]
			if i < (self.numberRows - 1):
				print(rawString, end=ROW_DELIMITER)
			else:
				print(rawString, end="")

	def printFrequency(self, currentColumn):
		if NAMED_COLUMNS:
			currentColumn = self.columnNames.index(currentColumn)
		maxValue = 5
		maxCount = 9
		for item in self.frequencyValues[currentColumn]:
			maxValue = max(maxValue, len(str(item[0])))
			maxCount = max(maxCount, len(str(item[1])))
		formatString = "{:<" + str(maxValue) + "} {:<" + str(maxCount) + "}"
		print(formatString.format("Value", "Frequency"))
		for item in self.frequencyValues[currentColumn]:
			print(formatString.format(item[0], item[1]))

	def sortLog(self, currentColumn, reverseSort=False):
		newData = []
		if NAMED_COLUMNS:
			currentColumn = self.columnNames.index(currentColumn)
			newData.append(self.columnNames)

		# See top of file for details on this
		global sortHack
		sortHack = currentColumn
		newData.extend(sorted(self.rawData, key=functools.cmp_to_key(compareStrings), reverse=reverseSort))

		return DataFrame(newData)

	def combineColumns(self, toCombine, columnDelimiter):
		newData = []
		firstValue = 0
		if NAMED_COLUMNS:
			for i in range(len(toCombine)):
				toCombine[i] = self.columnNames.index(toCombine[i])
			firstValue = toCombine[0]
			toCombine.pop(0)
			toCombine.sort(reverse=True)
			newRow = self.columnNames.copy()
			newString = self.columnNames[firstValue]
			for i in range(len(toCombine)):
				newString = newString + columnDelimiter + self.columnNames[toCombine[i]]
			newRow[firstValue] = newString
			for i in range(len(toCombine)):
				newRow.pop(toCombine[i])
			newData.append(newRow)
		else:
			firstValue = toCombine[0]
			toCombine.pop(0)
			toCombine.sort(reverse=True)
		for i in range(self.numberRows):
			newRow = self.rawData[i].copy()
			newString = self.rawData[i][firstValue]
			for j in range(len(toCombine)):
				newString = newString + columnDelimiter + self.rawData[i][toCombine[j]]
			newRow[firstValue] = newString
			for i in range(len(toCombine)):
				newRow.pop(toCombine[i])
			newData.append(newRow)
		return DataFrame(newData)

	def splitColumn(self, currentColumn, columnDelimiter):
		if NAMED_COLUMNS:
			currentColumn = self.columnNames.index(currentColumn)
		splitValues = []
		maxSplit = 0
		for i in range(self.numberRows):
			splitValues.append(self.rawData[i][currentColumn].split(columnDelimiter))
			maxSplit = max(maxSplit, len(splitValues[i]))
		for i in range(self.numberRows):
			for j in range(maxSplit - len(splitValues[i])):
				splitValues[i].append("")
		newData = []
		if NAMED_COLUMNS:
			newRow = []
			for i in range(self.numberColumns):
				if i == currentColumn:
					for j in range(maxSplit):
						newRow.append(self.columnNames[i] + " " + str(j))
				else:
					newRow.append(self.columnNames[i])
			newData.append(newRow)
		for i in range(self.numberRows):
			newRow = []
			for j in range(self.numberColumns):
				if j == currentColumn:
					for k in range(maxSplit):
						newRow.append(splitValues[i][k])
				else:
					newRow.append(self.rawData[i][j])
			newData.append(newRow)
		return DataFrame(newData)

	def removeColumn(self, currentColumn):
		newData = []
		if NAMED_COLUMNS:
			currentColumn = self.columnNames.index(currentColumn)
			newRow = self.columnNames.copy()
			newRow.pop(currentColumn)
			newData.append(newRow)
		for i in range(len(self.rawData)):
			newRow = self.rawData[i].copy()
			newRow.pop(currentColumn)
			newData.append(newRow)
		return DataFrame(newData)

	def combineLogs(self, other):
		newData = []
		if NAMED_COLUMNS:
			newRow = []
			for i in range(max(self.numberColumns, other.numberColumns)):
				if i < self.numberColumns:
					newRow.append(self.columnNames[i])
				else:
					newRow.append(other.columnNames[i])
			newData.append(newRow)
		newData.extend(self.rawData)
		newData.extend(other.rawData)
		for i in range(len(newData)):
			if len(newData[i]) < max(self.numberColumns, other.numberColumns):
				for j in range(max(self.numberColumns, other.numberColumns) - min(self.numberColumns, other.numberColumns)):
					newData[i].append("")
		return DataFrame(newData)

	def filterLog(self, currentColumn, conditionType, conditionValue):
		newData = []
		if NAMED_COLUMNS:
			currentColumn = self.columnNames.index(currentColumn)
			newData.append(self.columnNames)
		for i in range(self.numberRows):

			# See top of file for details on this
			global sortHack
			sortHack = 0
			compareResult = compareStrings([self.rawData[i][currentColumn]], [conditionValue])

			if conditionType == "<" and compareResult == -1:
				newData.append(self.rawData[i])			
			elif conditionType == ">" and compareResult == 1:
				newData.append(self.rawData[i])
			elif conditionType == "==" and compareResult == 0:
				newData.append(self.rawData[i])
			elif conditionType == "<=" and compareResult < 1:
				newData.append(self.rawData[i])
			elif conditionType == ">=" and compareResult > -1:
				newData.append(self.rawData[i])
			elif conditionType == "!=" and compareResult != 0:
				newData.append(self.rawData[i])
			elif conditionType == "contains" and str(conditionValue) in self.rawData[i][currentColumn]:
				newData.append(self.rawData[i])
			elif conditionType == "matches" and re.compile(conditionValue).match(self.rawData[i][currentColumn]):
				newData.append(self.rawData[i])
		return DataFrame(newData)

mainData = None

with open(FILE_NAME) as f:
	rawData = f.read().split(ROW_DELIMITER)
	if rawData[-1] == "":
		rawData = rawData[:-1]
	for i in range(len(rawData)):
		rawData[i] = rawData[i].split(COLUMN_DELIMITER)
	mainData = DataFrame(rawData)

# Begin main

mainData.completePrint()
