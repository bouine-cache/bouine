package consensus

import (
	"testing"

	"github.com/gofiber/fiber/v2/utils"
)

func Test_ForwardToLeader_AsLeader(t *testing.T) {
	t.Parallel()

	config := Config{
		Leader: "leader",
	}

	utils.AssertEqual(t, ForwardToLeader(config), ErrForwardToLeaderAsLeader)
}
func Test_ForwardToLeader_AsFollower(t *testing.T) {
	t.Parallel()

	config := Config{
		Leader: "follower",
	}

	utils.AssertEqual(t, ForwardToLeader(config), nil)
}

func Test_ForwardToLeader_AsCandidate(t *testing.T) {
	t.Parallel()

	config := Config{
		Leader: "candidate",
	}

	utils.AssertEqual(t, ForwardToLeader(config), nil)
}

func Test_IsLeader_AsLeader(t *testing.T) {
	t.Parallel()

	config := Config{
		Leader: "leader",
	}

	utils.AssertEqual(t, IsLeader(config), true)
}

func Test_IsLeader_AsFollower(t *testing.T) {
	t.Parallel()

	config := Config{
		Leader: "follower",
	}

	utils.AssertEqual(t, IsLeader(config), false)
}

func Test_IsLeader_AsCandidate(t *testing.T) {
	t.Parallel()

	config := Config{
		Leader: "candidate",
	}

	utils.AssertEqual(t, IsLeader(config), false)
}
