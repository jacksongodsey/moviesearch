package main

/*

imports to be used

*/
import (
	"bufio"
	"fmt"
	"os"
	"runtime"
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

func readLines(file *os.File) ([]string, error) {
	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return lines, nil
}

func readMovieData(filePath string, ratings map[string]struct {
	AverageRating float64
	NumVotes      int
}, numWorkers int) (map[string]Movie, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	movies := make(map[string]Movie)
	lines, err := readLines(file)
	if err != nil {
		return nil, err
	}

	var wg sync.WaitGroup
	movieChannel := make(chan Movie)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(lines []string) {
			defer wg.Done()
			for _, line := range lines {
				fields := strings.Split(line, "\t")
				if len(fields) >= 9 {
					tconst := fields[0]
					titleType := fields[1]
					if rating, ok := ratings[tconst]; ok && titleType == "movie" {
						movie := Movie{
							TitleId:       tconst,
							Title:         fields[2],
							AverageRating: rating.AverageRating,
							NumVotes:      rating.NumVotes,
							Genres:        fields[8],
						}
						movieChannel <- movie
					}
				}
			}
		}(lines[i*len(lines)/numWorkers : (i+1)*len(lines)/numWorkers])
	}

	go func() {
		wg.Wait()
		close(movieChannel)
	}()

	for movie := range movieChannel {
		movies[movie.TitleId] = movie
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

func main() {
	// Read ratings data first
	fmt.Println("Stuff is happening starting time now.")
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
	var memStatsStart runtime.MemStats
	runtime.ReadMemStats((&memStatsStart))
	fmt.Printf("Starting memory usage: %.2f MiB\n", float64(memStatsStart.Alloc)/1024/1024)
	movieData, err := readMovieData("filtered_output_file.tsv", ratings, 16)
	if err != nil {
		fmt.Println("Error reading movie data:", err)
		return
	}
	var memStatsEnd runtime.MemStats
	runtime.ReadMemStats(&memStatsEnd)
	fmt.Printf("Final memory usage: %.2f MiB\n", float64(memStatsEnd.Alloc)/1024/1024)
	fmt.Printf("Memory used by function: %.2f MiB\n", float64(memStatsEnd.Alloc-memStatsStart.Alloc)/1024/1024)
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

	var menu string

	for {
		fmt.Println("search - search for a movie by title")
		fmt.Println("quit or q - quit")
		fmt.Print("Enter your choice: ")
		fmt.Scanln(&menu)

		switch strings.ToLower(menu) {
		case "search":
			reader := bufio.NewReader(os.Stdin)
			fmt.Print("Please enter the movie you'd like to search exactly as it was published: ")
			titleToSearch, _ := reader.ReadString('\n')
			titleToSearch = strings.TrimSpace(titleToSearch)
			temp := time.Now()
			foundMovie, found := binarySearchNon(movieSlice, titleToSearch)
			tempTime := time.Since(temp)

			if found {
				fmt.Printf("Movie found: Title: %s, Rating: %f, NumVotes: %d, Genres: %s\n",
					foundMovie.Title, foundMovie.AverageRating, foundMovie.NumVotes, foundMovie.Genres)
			} else {
				fmt.Println("Movie not found.")
			}

			fmt.Printf("Time without concurrency %s\n", tempTime)
		case "q", "quit":
			fmt.Println("Quiting...")
			return
		default:
			fmt.Println("Invalid choice, please try again.")
		}
	}

}
