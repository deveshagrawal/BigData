from mrjob.job import MRJob
from mrjob.step import MRStep

def MappedMovieRatings(MRJob):
    def steps(self):
        return [
            MRStep(
            mapper=step.MapperGetRatings,
            reducer=self.ReducerCountRatings
            )]

    #sample input of dataRow would look like: 1  'Inception'  4  '2018-01-01'
    def MapperGetRatings(self, _, dataRow):
        (movieID, movieName, rating, timestamp) = dataRow.split('\t')
        yield rating, 1

    def ReducerCountRatings(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    MappedMovieRatings.run()
