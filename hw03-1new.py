from mrjob.job import MRJob


class AvgNumReview(MRJob):
    def mapper(self, _, line):
        (business_id, date, review_id, stars, text, type, user_id, cool, useful, funny) = line.split('t')
        yield "words", len(text.split())

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    AvgNumReview.run()

