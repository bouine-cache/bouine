package middlewares

import (
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/proxy"
	"github.com/gofiber/fiber/v2/utils"
	"github.com/thylong/bouine/internal/consensus"
)

func Test_Consensus_WriteAsLeader(t *testing.T) {
	t.Parallel()

	// Must not forward to leader
	target, addr := createBouineLeaderServer(ConsensusMiddleware(consensus.Config{Leader: "leader"}), t)

	resp, err := target.Test(httptest.NewRequest("GET", "/", nil), 2000)
	// Next middleware in chain should be invoked
	// which results in a 404 as there is no other middleware or handler in the chain
	utils.AssertEqual(t, nil, err)
	utils.AssertEqual(t, fiber.StatusNotFound, resp.StatusCode)

	app := fiber.New(fiber.Config{DisableStartupMessage: true})

	app.Use(proxy.Balancer(proxy.Config{Servers: []string{addr}}))

	req := httptest.NewRequest("GET", "/", nil)
	req.Host = addr
	resp, err = app.Test(req)
	// => Next middleware in chain should be invoked
	// which results in a 404 as there is no other middleware or handler in the chain
	utils.AssertEqual(t, nil, err)
	utils.AssertEqual(t, fiber.StatusNotFound, resp.StatusCode)
}

func Test_Consensus_WriteAsFollower(t *testing.T) {
	t.Parallel()

	target, addr := createBouineLeaderServer(ConsensusMiddleware(consensus.Config{Leader: "follower"}), t)

	resp, err := target.Test(httptest.NewRequest("GET", "/", nil), 2000)
	// Must forward to leader
	// which results in a 200 for the moment
	utils.AssertEqual(t, nil, err)
	utils.AssertEqual(t, fiber.StatusOK, resp.StatusCode)

	app := fiber.New(fiber.Config{DisableStartupMessage: true})

	app.Use(proxy.Balancer(proxy.Config{Servers: []string{addr}}))

	req := httptest.NewRequest("GET", "/", nil)
	req.Host = addr
	resp, err = app.Test(req)
	// Must forward to leader
	// which results in a 200 for the moment
	utils.AssertEqual(t, nil, err)
	utils.AssertEqual(t, fiber.StatusOK, resp.StatusCode)
}

func Test_Consensus_WriteAsCandidate(t *testing.T) {
	t.Parallel()

	// TODO: ?
}
