package requests

type Header struct {
	Post				int     `json:"Post"`
	IsMarkedForDeletion	*bool	`json:"IsMarkedForDeletion"`
}
