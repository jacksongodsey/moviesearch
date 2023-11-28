package main

/*

imports to be used

*/
import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

/*
creating a movie struct with the appropriate fields
*/
type Movie struct {
	TitleId       string
	Title         string
	AverageRating float64
	NumVotes      int
	Genres        string
}

/*

Takes a filepath, and the ratings map as input.
opens the file and creates a file object.
we then read through the file using bufio, and create movie object in the map.
This is only done if their is a rating to go with it, and we compare the ttconst variable to make sure this happens.
we then return the map

*/

func readMovieData(filePath string, ratings map[string]struct {
	AverageRating float64
	NumVotes      int
}) (map[string]Movie, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	movies := make(map[string]Movie)
	scanner := bufio.NewScanner(file)
	scanner.Scan() // Skip the header line
	for scanner.Scan() {
		fields := strings.Split(scanner.Text(), "\t")
		if len(fields) >= 9 {
			tconst := fields[0]
			titleType := fields[1]
			if rating, ok := ratings[tconst]; ok && titleType == "movie" { // Check if a rating exists for the movie and titleType is not "movie"
				movie := Movie{
					TitleId:       tconst,
					Title:         fields[2],
					AverageRating: rating.AverageRating,
					NumVotes:      rating.NumVotes,
					Genres:        fields[8],
				}
				movies[tconst] = movie
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return movies, nil
}

/*

This function is very similar to the movie function. it creates a rating map and returns it after scanning throuhg a file

*/

func readRatingData(filePath string) (map[string]struct {
	AverageRating float64
	NumVotes      int
}, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	ratings := make(map[string]struct {
		AverageRating float64
		NumVotes      int
	})
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Split(scanner.Text(), "\t")
		if len(fields) >= 3 {
			tconst := fields[0]
			rating := averageRating(fields[1])
			votes := parseInt(fields[2])
			ratings[tconst] = struct {
				AverageRating float64
				NumVotes      int
			}{rating, votes}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return ratings, nil
}

// parse an average rating
func averageRating(s string) float64 {
	rating, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0.0
	}
	return rating
}

// parse a string and return it as an int
func parseInt(s string) int {
	num, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}
	return num
}

// standard heap sort implementation. Compares the movie titles and sorts based off that.
func heapify(movies []Movie, n int, i int) {
	largest := i
	left := 2*i + 1
	right := 2*i + 2

	if left < n && movies[left].Title > movies[largest].Title {
		largest = left
	}

	if right < n && movies[right].Title > movies[largest].Title {
		largest = right
	}

	if largest != i {
		movies[i], movies[largest] = movies[largest], movies[i]
		heapify(movies, n, largest)
	}
}

func heapSort(movies []Movie) {
	n := len(movies)

	for i := n/2 - 1; i >= 0; i-- {
		heapify(movies, n, i)
	}

	for i := n - 1; i >= 0; i-- {
		movies[0], movies[i] = movies[i], movies[0]
		heapify(movies, i, 0)
	}
}

func binarySearchNon(movies []Movie, title string) (Movie, bool) {
	low, high := 0, len(movies)-1

	for low <= high {
		mid := (low + high) / 2
		if movies[mid].Title == title {
			return movies[mid], true
		} else if movies[mid].Title < title {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	return Movie{}, false
}

// concurrent binary serach using goroutines. Allows us to create concurrent workers to look through smaller sections of the map. then send the result through a channel.
func binarySearch(movies []Movie, title string, wg *sync.WaitGroup, resultChan chan Movie) {
	defer wg.Done()

	low, high := 0, len(movies)-1

	for low <= high {
		mid := (low + high) / 2
		if strings.EqualFold(movies[mid].Title, title) {
			resultChan <- movies[mid]
			return
		} else if strings.Compare(movies[mid].Title, title) < 0 {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	resultChan <- Movie{} // If not found, send an empty Movie
}

func main() {
	// Read ratings data first
	fmt.Println("Stuff is happening starting time now.")

	startTotalTime := time.Now()
	startTimeRank := time.Now()

	ratings, err := readRatingData("title.ratings.tsv")
	if err != nil {
		fmt.Println("Error reading ratings:", err)
		return
	}
	elapsedTimeRank := time.Since(startTimeRank)
	fmt.Printf("Time to map ratings: %s\n", elapsedTimeRank)

	// Then read movie data passing ratings map
	startTimeMovie := time.Now()
	movieData, err := readMovieData("filtered_output_file.tsv", ratings)
	if err != nil {
		fmt.Println("Error reading movie data:", err)
		return
	}
	elapsedTimeMovie := time.Since(startTimeMovie)
	fmt.Printf("Time to map movies: %s\n", elapsedTimeMovie)

	var movieSlice []Movie
	for _, movie := range movieData {
		movieSlice = append(movieSlice, movie)
	}

	// Sorting
	startTimeSort := time.Now()
	heapSort(movieSlice)
	elapsedTimeSort := time.Since(startTimeSort)
	fmt.Printf("Time to sort movies using heap sort: %s\n", elapsedTimeSort)

	titleToSearch := "Rick and Morty"
	numWorkers := 4 // creating 4 workers

	//creating a waiting group, and making a results channel.
	var wg sync.WaitGroup
	resultChan := make(chan Movie, numWorkers)

	// Searching
	startTimeSearch := time.Now()
	chunkSize := len(movieSlice) / numWorkers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)

		start := i * chunkSize
		end := start + chunkSize
		if i == numWorkers-1 {
			end = len(movieSlice)
		}

		go binarySearch(movieSlice[start:end], titleToSearch, &wg, resultChan)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// results
	var foundMovie Movie
	for result := range resultChan {
		if result.TitleId != "" { // Check if the result is not an empty Movie
			foundMovie = result
			break
		}
	}
	elapsedTimeSearch := time.Since(startTimeSearch)
	fmt.Printf("Time to search for your movie/tv show: %s\n", elapsedTimeSearch)

	temp := time.Now()
	foundMovie, found := binarySearchNon(movieSlice, titleToSearch)
	tempTime := time.Since(temp)
	fmt.Printf("Time without concurrency %s\n", tempTime)

	// Display result
	if found {
		fmt.Printf("Movie found: Title: %s, Rating: %f, NumVotes: %d, Genres: %s\n",
			foundMovie.Title, foundMovie.AverageRating, foundMovie.NumVotes, foundMovie.Genres)
	} else {
		fmt.Println("Movie not found.")
	}
	// display result
	if foundMovie.TitleId != "" {
		fmt.Printf("Movie/TV show found: Title: %s, Rating: %f, NumVotes: %d, Genres: %s\n",
			foundMovie.Title, foundMovie.AverageRating, foundMovie.NumVotes, foundMovie.Genres)
	} else {
		fmt.Println("Movie/TV show not found.")
	}

	endTotalTime := time.Since(startTotalTime)
	fmt.Printf("Total time elapsed: %s\n", endTotalTime)

}
