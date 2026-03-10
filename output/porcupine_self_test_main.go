package main

import (
	"fmt"
	"github.com/anishathalye/porcupine"
	"os"
)

func main() {
	goodHistory := []porcupine.Event{
		// Add known-good history events here
	}

	badHistory := []porcupine.Event{
		// Add known-bad history events here
	}

	model := porcupine.Model{
		Init: func() interface{} { return 0 },
		Step: func(state, input, output interface{}) (bool, interface{}) {
			// Define the model step function here
			return true, state
		},
	}

	goodResult := porcupine.CheckEvents(model, goodHistory)
	badResult := porcupine.CheckEvents(model, badHistory)

	if goodResult {
		fmt.Println("Known-good history: PASS")
	} else {
		fmt.Println("Known-good history: FAIL")
	}

	if !badResult {
		fmt.Println("Known-bad history: PASS")
	} else {
		fmt.Println("Known-bad history: FAIL")
	}

	os.Exit(0)
}
