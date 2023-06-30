# DataFrame Explorer

CLI and python library to explore dataframes.

![Demo](media/demo.gif)

## Usage

### From terminal

Run `dtex` by providing filenames:

```
dtex <filenames>
```

### From python

```sh
pip install git+https://github.com/Virgiel/dtex#subdirectory=py-dtex
```

```py
import polars as pl
import dtex

polars_lazy = (
    pl.scan_ipc("data/postcode.csv").groupby("code_postal").count().sort("count", descending=True)
)

dtex.ex([polars_lazy])
```

## Features

- Streaming design to open large files in ms
- Intelligent column sizing to reduce flicker when scrolling and make full use
  of terminal space
- duckdb integration

## Key bindings

| Key                  | Action       |
| -------------------- | ------------ |
| `Tab`                | Nest tab     |
| `Maj Tab`            | Previous tab |
| `Ctrl c` or `Ctrl d` | Exit         |

### Normal

| Key            | Action                     |
| -------------- | -------------------------- |
| `h` or `←`     | Move left                  |
| `l` or `→`     | Move right                 |
| `k` or `↑`     | Move up                    |
| `j` or `↓`     | Move down                  |
| `H` or `Maj ←` | Move window left           |
| `L` or `Maj →` | Move window right          |
| `K` or `Maj ↑` | Move window up             |
| `J` or `Maj ↓` | Move window down           |
| `g`            | Move to first row          |
| `G`            | Move to last row           |
| `d`            | Switch to description view |
| `s`            | Switch to sizing mode      |
| `p`            | Switch to projection mode  |
| `0-9`          | Switch to navigation mode  |
| `q`            | Close tab                  |

### Sizing

| Key          | Action                             |
| ------------ | ---------------------------------- |
| `Esc` or `s` | Return to normal mode              |
| `h` or `←`   | Reduce col size by one             |
| `k` or `→`   | Augment col size by one            |
| `k` or `↑`   | Free col size                      |
| `j` or `↓`   | Fit col size                       |
| `space`      | Toggle header/content fitting mode |
| `r`          | Reset sizing                       |
| `f`          | Fit all cols to their content      |

### Projection

| Key          | Action                |
| ------------ | --------------------- |
| `Esc` or `p` | Return to normal mode |
| `h` or `←`   | Move col left         |
| `l` or `→`   | Move col right        |
| `k` or `↑`   | Reset projection      |
| `j` or `↓`   | Hide col              |

### Navigation

| Key     | Action                                 |
| ------- | -------------------------------------- |
| `Esc`   | Return to normal mode and reset cursor |
| `Enter` | Return to normal mode and keep cursor  |
| other   | Write into prompt                      |

## TODO

- Better navigation mode
- Smart formatting of path ~ for home and ‥ instead of ..
- Better fs events handlinge
