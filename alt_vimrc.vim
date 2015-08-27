set nocompatible              " be iMproved, required
filetype off                  " required

" set the runtime path to include Vundle and initialize
set rtp+=~/.vim/bundle/Vundle.vim
call vundle#begin()
" alternatively, pass a path where Vundle should install plugins
"call vundle#begin('~/some/path/here')

" let Vundle manage Vundle, required
Plugin 'gmarik/Vundle.vim'


" vim
" Plugin 'davidhalter/jedi-vim'
Plugin 'scrooloose/nerdtree'
"Plugin 'Valloric/YouCompleteMe'
"Plugin 'myusuf3/numbers.vim'

Plugin 'bling/vim-airline'
Plugin 'elzr/vim-json'
"Plugin 'nathanaelkane/vim-indent-guides'
Plugin 'Yggdroot/indentLine'
Plugin 'kien/rainbow_parentheses.vim'

"Plugin 'tpope/vim-sensible'
"Plugin 'walm/jshint.vim'
"Plugin 'mustache/vim-mustache-handlebars'
"Plugin 'nvie/vim-flake8'

" Git
Plugin 'tpope/vim-fugitive'
Plugin 'airblade/vim-gitgutter.git'

" python
" Plugin 'klen/python-mode'
" Plugin 'hynek/vim-python-pep8-indent'
" Plugin 'sophacles/vim-bundle-mako'

" haskell
Plugin 'dag/vim2hs'

" scala
Bundle 'derekwyatt/vim-scala'

"Plugin 'wincent/Command-T'
"Plugin 'ervandew/supertab'
"Plugin 'kevinw/pyflakes-vim'

" go
Plugin 'fatih/vim-go'

" Markdown
Plugin 'godlygeek/tabular'
Plugin 'plasticboy/vim-markdown'


" Color Schemes
Plugin 'altercation/vim-colors-solarized'
Plugin 'sjl/badwolf'
Plugin 'benjaminwhite/Benokai'
Plugin 'chriskempson/base16-vim'

"altercation/vim-colors-solarized'
" The following are examples of different formats supported.
" Keep Plugin commands between vundle#begin/end.
" plugin on GitHub repo
"Plugin 'tpope/vim-fugitive'
" plugin from http://vim-scripts.org/vim/scripts.html
"Plugin 'L9'

" Brief help
" :PluginList       - lists configured plugins
" :PluginInstall    - installs plugins; append `!` to update or just :PluginUpdate
" :PluginSearch foo - searches for foo; append `!` to refresh local cache
" :PluginClean      - confirms removal of unused plugins; append `!` to auto-approve removal
"
" see :h vundle for more details or wiki for FAQ
" Put your non-Plugin stuff after this line

call vundle#end()            " required
filetype plugin indent on    " required
" To ignore plugin indent changes, instead use:
"filetype plugin on

"let base16colorspace=256  " Access colors present in 256 colorspace

" ========== GENERAL VIM SETTINGS ==========
syntax enable

"let g:solarized_termtrans = 1
set background=dark
" set background=light
"colorscheme solarized
"colorscheme badwolf
"colorschem Benokai
"colorschem Spacedust
colorscheme base16-default

"set number                    " Display line numbers
set tabstop=2
set shiftwidth=2
set softtabstop=2
set expandtab
set autoindent
set smarttab
filetype indent on
filetype on
filetype plugin on


set showmatch  " Show matching brackets.
set matchtime=5  " Bracket blinking.
set ruler  " Show ruler
set cursorline " highlight current line.
set showcmd " Display an incomplete command in the lower right corner of the Vim window


set foldenable " Turn on folding
set foldmethod=marker " Fold on the marker
set foldlevel=100 " Don't autofold anything (but I can still fold manually)
set foldopen=block,hor,mark,percent,quickfix,tag " what movements open folds


" Enable search highlighting
set hlsearch

" Use F11 to toggle between paste and nopaste
"set pastetoggle=


set colorcolumn=80






" VIM airline config
set laststatus=2                " Solves: statusline does not appear until I create a split
set noshowmode                  " Get rid of the default mode indicator
"let g:airline_powerline_fonts = 1   " Use powerline symbols
let g:arline_theme = 'light'    " Self explanatory
" End of airline configs


let g:airline_theme= 'molokai'

" Python-mode
" Activate rope
" Keys:
" K             Show python docs
" <Ctrl-Space>  Rope autocomplete
" <Ctrl-c>g     Rope goto definition
" <Ctrl-c>d     Rope show documentation
" <Ctrl-c>f     Rope find occurrences
" <Leader>b     Set, unset breakpoint (g:pymode_breakpoint enabled)
" [[            Jump on previous class or function (normal, visual, operator modes)
" ]]            Jump on next class or function (normal, visual, operator modes)
" [M            Jump on previous class or method (normal, visual, operator modes)
" ]M            Jump on next class or method (normal, visual, operator modes)
"let g:pymode_rope = 0

" Documentation
"let g:pymode_doc = 1
"let g:pymode_doc_key = 'K'

"Linting
"let g:pymode_lint = 1
"let g:pymode_lint_checker = "pyflakes,pep8"
" Auto check on save
"let g:pymode_lint_write = 0

" Support virtualenv
"let g:pymode_virtualenv = 1

" Enable breakpoints plugin
"let g:pymode_breakpoint = 1
"let g:pymode_breakpoint_key = '<leader>b'

" syntax highlighting
"let g:pymode_syntax = 1
"let g:pymode_syntax_all = 1
"let g:pymode_syntax_indent_errors = g:pymode_syntax_all
"let g:pymode_syntax_space_errors = g:pymode_syntax_all

" Don't autofold code
"let g:pymode_folding = 0
"let g:pymode_indent = 0
