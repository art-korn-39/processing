package processing

import "time"

type Period struct {
	startDay time.Time
	endDay   time.Time
}

func GetChannelOfDays(startDate, finishDate time.Time, duration time.Duration) chan Period {

	channel := make(chan Period, 50)

	go func() {
		startDay := startDate
		for {
			if startDay.After(finishDate) {
				break
			}

			endDay := startDay.Round(duration).Add(duration).Add(-1 * time.Second) //23:59:59
			if endDay.After(finishDate) {
				endDay = finishDate
			}

			period := Period{
				startDay: startDay,
				endDay:   endDay,
			}
			channel <- period

			startDay = startDay.Add(duration)
		}
		close(channel)
	}()

	return channel

}
