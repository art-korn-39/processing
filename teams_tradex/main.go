package teams_tradex

import (
	"app/logs"
	"app/querrys"
	"app/util"
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
)

var (
	map_team_names map[string]*Team
	map_team_ids   map[string]*Team
)

func init() {
	map_team_names = map[string]*Team{}
	map_team_ids = map[string]*Team{}
}

func Read(db *sqlx.DB) {

	if db == nil {
		return
	}

	start_time := time.Now()

	stat := querrys.Stat_Select_teams_tradex()

	slice_teams := []Team{}

	err := db.Select(&slice_teams, stat)
	if err != nil {
		logs.Add(logs.INFO, err)
		return
	}

	for _, team := range slice_teams {

		map_team_names[team.Name] = &team
		map_team_ids[strings.ToLower(team.Id)] = &team

	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение teams tradex из Postgres: %v [%s строк]", time.Since(start_time), util.FormatInt(len(slice_teams))))

}

func GetTeamByName(name string) (*Team, bool) {

	val, ok := map_team_names[name]
	if ok {
		return val, true
	}

	return nil, false

}

func GetTeamByTeamID(team_id string) (*Team, bool) {

	val, ok := map_team_ids[strings.ToLower(team_id)]
	if ok {
		return val, true
	}

	return nil, false

}
