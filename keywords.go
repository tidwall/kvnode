package roam

type keyword int

const (
	kwUnknown keyword = iota
	kwSET
	kwGET
	kwDEL
	kwKEYS
	kwDUMP
	kwDELIF
	kwLISTEX
	kwFLUSHDB
	kwSHUTDOWN
)

func pkeyword(b []byte) keyword {
	switch len(b) {
	case 3:
		if (b[0] == 's' || b[0] == 'S') &&
			(b[1] == 'e' || b[1] == 'E') &&
			(b[2] == 't' || b[2] == 'T') {
			return kwSET
		}
		if (b[0] == 'g' || b[0] == 'G') &&
			(b[1] == 'e' || b[1] == 'E') &&
			(b[2] == 't' || b[2] == 'T') {
			return kwGET
		}
		if (b[0] == 'd' || b[0] == 'D') &&
			(b[1] == 'e' || b[1] == 'E') &&
			(b[2] == 'l' || b[2] == 'L') {
			return kwDEL
		}
	case 4:
		if (b[0] == 'k' || b[0] == 'K') &&
			(b[1] == 'e' || b[1] == 'E') &&
			(b[2] == 'y' || b[2] == 'Y') &&
			(b[3] == 's' || b[3] == 'S') {
			return kwKEYS
		}
		if (b[0] == 'd' || b[0] == 'D') &&
			(b[1] == 'u' || b[1] == 'U') &&
			(b[2] == 'm' || b[2] == 'M') &&
			(b[3] == 'p' || b[3] == 'P') {
			return kwDUMP
		}
	case 5:
		if (b[0] == 'd' || b[0] == 'D') &&
			(b[1] == 'e' || b[1] == 'E') &&
			(b[2] == 'l' || b[2] == 'L') &&
			(b[3] == 'i' || b[3] == 'I') &&
			(b[4] == 'f' || b[4] == 'F') {
			return kwDELIF
		}
	case 6:
		if (b[0] == 'l' || b[0] == 'L') &&
			(b[1] == 'i' || b[1] == 'I') &&
			(b[2] == 's' || b[2] == 'S') &&
			(b[3] == 't' || b[3] == 'T') &&
			(b[4] == 'e' || b[4] == 'E') &&
			(b[5] == 'x' || b[5] == 'X') {
			return kwLISTEX
		}
	case 7:
		if (b[0] == 'f' || b[0] == 'F') &&
			(b[1] == 'l' || b[1] == 'L') &&
			(b[2] == 'u' || b[2] == 'U') &&
			(b[3] == 's' || b[3] == 'S') &&
			(b[4] == 'h' || b[4] == 'H') &&
			(b[5] == 'd' || b[5] == 'D') &&
			(b[6] == 'b' || b[6] == 'B') {
			return kwFLUSHDB
		}
	case 8:
		if (b[0] == 's' || b[0] == 'S') &&
			(b[1] == 'h' || b[1] == 'H') &&
			(b[2] == 'u' || b[2] == 'U') &&
			(b[3] == 't' || b[3] == 'T') &&
			(b[4] == 'd' || b[4] == 'D') &&
			(b[5] == 'o' || b[5] == 'O') &&
			(b[6] == 'w' || b[6] == 'W') &&
			(b[7] == 'n' || b[7] == 'N') {
			return kwSHUTDOWN
		}
	}
	return kwUnknown
}
