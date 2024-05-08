package requests

type Friend struct {
	Post				int     `json:"Post"`
	Friend				int     `json:"Friend"`
	IsMarkedForDeletion	*bool	`json:"IsMarkedForDeletion"`
}
