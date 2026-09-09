package main

import "net/http"

// agentCatalog is the information needed to start a new conversation. The
// daemon address belongs here too: it makes clear which daemon supplied the
// choices when one UI server is used to front more than one deployment.
type agentCatalog struct {
	Daemon   string         `json:"daemon"`
	Projects []agentProject `json:"projects"`
}

type agentProject struct {
	ProjectID string        `json:"projectId"`
	Name      string        `json:"name"`
	Agents    []agentChoice `json:"agents"`
}

type agentChoice struct {
	AgentName    string `json:"agentName"`
	DisplayName  string `json:"displayName,omitempty"`
	Description  string `json:"description,omitempty"`
	Enabled      bool   `json:"enabled"`
	Availability string `json:"availability,omitempty"`
	Selectable   bool   `json:"selectable"`
}

// agents lists the projects and agents the user can choose for a new chat.
//
// The catalog comes from the SDK rather than from Connect calls made here: a
// product that has to pick a counterpart before it can start a conversation is
// doing something every chat product does, and going around the SDK for it
// meant this server carried its own idea of the wire.
func (s *uiServer) agents(w http.ResponseWriter, r *http.Request) {
	projects, err := s.client.Projects(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	catalog := agentCatalog{Daemon: s.daemon, Projects: make([]agentProject, 0, len(projects))}
	for _, project := range projects {
		entry := agentProject{ProjectID: project.ID, Name: project.Name}
		for _, agent := range project.Agents {
			entry.Agents = append(entry.Agents, agentChoice{
				AgentName:    agent.Name,
				DisplayName:  agent.DisplayName,
				Description:  agent.Description,
				Enabled:      agent.Available,
				Availability: agent.Unavailable,
				Selectable:   agent.Available,
			})
		}
		catalog.Projects = append(catalog.Projects, entry)
	}
	writeJSON(w, catalog)
}
