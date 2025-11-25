package registration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type RegisterRequest struct {
	Token string `json:"token"`
}

type RegisterResponse struct {
	OrgId string `json:"id"`
	Name  string `json:"name"`
}

func Register(orgApiUrl, bootstrapToken string) (string, string, error) {
	reqBody := RegisterRequest{Token: bootstrapToken}
	b, _ := json.Marshal(reqBody)

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, orgApiUrl+"/api/v1/portal/auth/notifi-agent/register-org", bytes.NewReader(b))
	if err != nil {
		return "", "", err
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("registration failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("registration failed with status: %s", resp.Status)
	}

	var r RegisterResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return "", "", err
	}
	if r.OrgId == "" {
		return "", "", fmt.Errorf("registration failed: empty OrgId")
	}
	return r.OrgId, r.Name, nil
}
