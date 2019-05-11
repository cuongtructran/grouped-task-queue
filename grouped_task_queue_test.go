package gtq

import (
	"reflect"
	"testing"
	"time"
)

func TestDispatchQueue(t *testing.T) {
	t.Run("ShouldExecuteInSequence", func(t *testing.T) {
		queue := NewGroupedDispatchQueue(5)
		defer queue.Destroy()

		var resultGOOG []string
		var resultAAPL []int
		queue.Submit("GOOG", func() { resultGOOG = append(resultGOOG, "A") })
		queue.Submit("AAPL", func() { resultAAPL = append(resultAAPL, 1) })
		queue.Submit("GOOG", func() { resultGOOG = append(resultGOOG, "B") })
		queue.Submit("AAPL", func() { resultAAPL = append(resultAAPL, 2) })
		queue.Submit("GOOG", func() { resultGOOG = append(resultGOOG, "C") })

		time.Sleep(1 * time.Second)
		if len(resultGOOG) != 3 {
			t.Errorf("Expected 3 got %d", len(resultGOOG))
		}

		if !reflect.DeepEqual([]string{"A", "B", "C"}, resultGOOG) {
			t.Errorf("Order of GOOG data doesn't match")
		}

		if len(resultAAPL) != 2 {
			t.Errorf("Expected 2 got %d", len(resultAAPL))
		}

		if !reflect.DeepEqual([]int{1, 2}, resultAAPL) {
			t.Errorf("Order of AAPL data doesn't match")
		}
	})

	t.Run("ShouldReturnErrorWhenGroupIsFull", func(t *testing.T) {
		queue := NewGroupedDispatchQueue(5)
		defer queue.Destroy()

		for i := 1; i <= 5; i++ {
			queue.Submit("GOOG", func() { time.Sleep(5 * time.Minute) })
		}

		error := queue.Submit("GOOG", func() { time.Sleep(5 * time.Minute) })
		if "Group GOOG is full, can't add more tasks" != error.Error() {
			t.Errorf("Error didn't throw when queue is full")
		}
	})
}
